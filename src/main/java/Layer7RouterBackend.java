
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.kohsuke.args4j.CmdLineParser;
import org.xnio.ByteBufferPool;
import org.xnio.ChannelListener;
import org.xnio.CustomByteBufferPool;
import org.xnio.OptionMap;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;

import com.boxymoron.request.Request8;

/**
 * TODO: Look into cpu/thread affinity.
 * @author royer
 *
 */
public final class Layer7RouterBackend extends Common {
	
	final static Logger log = Logger.getLogger(Layer7RouterBackend.class);
	
	final static boolean isInfo=log.isInfoEnabled();
	final static boolean isDebug=log.isDebugEnabled();
	final static boolean isTrace=log.isTraceEnabled();

	final static AtomicInteger totalAccepted = new AtomicInteger();
	final static AtomicInteger sessionsCount = new AtomicInteger();
	final static AtomicLong globalClientWriteBytes = new AtomicLong();
	final static AtomicLong globalClientReadBytes = new AtomicLong();

	final static Xnio xnio = Xnio.getInstance();
	static XnioWorker worker;
	static OptionMap xnioOptions;
	//static ByteBufferSlicePool pool = new ByteBufferSlicePool(1024*8, 32*1024*1024*32);
	static ByteBufferPool pool;
	
	final static Options routerOptions = new Options();

	public static void main(String[] args) throws Exception {
		final CmdLineParser cmdLineParser = new CmdLineParser(routerOptions);
		cmdLineParser.parseArgument(args);
		System.out.println(routerOptions.toString());

		pool = CustomByteBufferPool.allocatePool(routerOptions.buffer_size);
		xnioOptions = OptionMap.builder()
				.set(org.xnio.Options.ALLOW_BLOCKING, false)
				.set(org.xnio.Options.RECEIVE_BUFFER, routerOptions.buffer_size)
				.set(org.xnio.Options.SEND_BUFFER, routerOptions.buffer_size)
				//.set(org.xnio.Options.READ_TIMEOUT, 30000)
				//.set(org.xnio.Options.WRITE_TIMEOUT, 30000)
				.set(org.xnio.Options.USE_DIRECT_BUFFERS, true)
				.set(org.xnio.Options.WORKER_IO_THREADS, routerOptions.num_threads)
				.set(org.xnio.Options.SPLIT_READ_WRITE_THREADS, false)
				.set(org.xnio.Options.BACKLOG, 1024 * 4)
				.set(org.xnio.Options.KEEP_ALIVE, false)
				.set(org.xnio.Options.TCP_NODELAY, true)
				.set(org.xnio.Options.CORK, true)
				.set(org.xnio.Options.REUSE_ADDRESSES, true)
				.getMap();

		worker = xnio.createWorker(xnioOptions);

		final Deque<FrontendReadListener> readListeners = new ConcurrentLinkedDeque<>();

		final ChannelListener<AcceptingChannel<StreamConnection>> acceptListener2 = getAcceptListener(readListeners);

		final AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(new InetSocketAddress(routerOptions.listen_port), acceptListener2, OptionMap.EMPTY);
		server.resumeAccepts();

		if(isInfo)log.info("Listening on " + server.getLocalAddress());
		
		setupConnectionReaper(readListeners);
		
		printStatistics();
	}

	private static void printStatistics() throws InterruptedException {
		final Map <Long, Long> workerCpuTimes = new LinkedHashMap<Long, Long>();
		final Runtime runtime = Runtime.getRuntime();
		final BufferPoolMXBean bufferPoolBean = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream().filter(bb->"direct".equals(bb.getName())).findFirst().get();
		final MemoryMXBean mmxb = ManagementFactory.getMemoryMXBean();
		final ThreadMXBean tmxb = getWorkerCpuTimes(workerCpuTimes);
		
		int acceptedLast = totalAccepted.get();
		long clientToBackendLast = globalClientReadBytes.get();
		long backendToClientLast = globalClientWriteBytes.get();
		long start = System.currentTimeMillis();
		String lastFormatted="";
		while(true){
			Thread.sleep(2000);
			long slept = System.currentTimeMillis() - start;

			final StringBuilder cpuStats = getCpuStats(tmxb, workerCpuTimes, ((double)slept)/1000d);
			final StringBuilder memoryStats = getMemoryStats(runtime, mmxb);
			
			double acceptedPerSec = ((double)totalAccepted.get() - (double)acceptedLast) / ((double)slept/1000d);
			double backendToClientPerSec = (globalClientWriteBytes.get() - backendToClientLast) / ((double)slept/1000d);
			String backendToClientPerSecUnits = "KB";
			if(backendToClientPerSec > ((1024*1024)-1)) {
				backendToClientPerSecUnits = "MB";
				backendToClientPerSec = backendToClientPerSec / (1024f*1024f);
			}else {
				backendToClientPerSec = backendToClientPerSec / 1024f;
			}
			
			double clientToBackendPerSec = (globalClientReadBytes.get() - clientToBackendLast) / ((double)slept/1000d);
			String clientToBackendPerSecUnits = "KB";
			if(clientToBackendPerSec > ((1024*1024)-1)) {
				clientToBackendPerSecUnits = "MB";
				clientToBackendPerSec = clientToBackendPerSec / (1024f*1024f);
			}else {
				clientToBackendPerSec = clientToBackendPerSec / 1024f;
			}
			
			final String formatted = String.format("Sess: %,.1f per/sec, %,d total, %,d curr, %,d FR, %,d FW, in %,.1f %s/sec, out %,.1f %s/sec, Direct %,.1f MB, %s, %s", 
					acceptedPerSec, totalAccepted.get(), sessionsCount.get(), globalClientReadBytes.get(), globalClientWriteBytes.get(), clientToBackendPerSec, clientToBackendPerSecUnits, backendToClientPerSec, backendToClientPerSecUnits, ((float)bufferPoolBean.getMemoryUsed())/(1024f*1024f), memoryStats, cpuStats);
			if(!formatted.equals(lastFormatted)){
				System.out.println(formatted);
			}
			lastFormatted = formatted;
			acceptedLast = totalAccepted.get();
			clientToBackendLast = globalClientReadBytes.get();
			backendToClientLast = globalClientWriteBytes.get();
			start = System.currentTimeMillis();
		}
	}

	private static void setupConnectionReaper(final Deque<FrontendReadListener> readListeners) {
		final Thread reaper = new Thread(new Thread(){
			public void run(){
				while(true){
					try{
						Thread.sleep(2000);
						final Iterator<FrontendReadListener> iter = readListeners.iterator();
						while(iter.hasNext()){
							FrontendReadListener listener = iter.next();
							if(!listener.checkLiveness()){
								iter.remove();
							}else{
								//if(isDebug)log.debug(listener.toString());
							}
						}
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}
		});
		reaper.setName("Idle Connection Reaper");
		reaper.start();
	}

	private static ChannelListener<AcceptingChannel<StreamConnection>> getAcceptListener(final Deque<FrontendReadListener> readListeners) {
		final ChannelListener<AcceptingChannel<StreamConnection>> acceptListener2 = new ChannelListener<AcceptingChannel<StreamConnection>>() {
			@Override
			public final void handleEvent(AcceptingChannel<StreamConnection> channel) {
				try {
					StreamConnection accepted;
					while ((accepted = channel.accept()) != null) {
						if(isDebug)log.debug("Accepted: " + accepted.getPeerAddress());
						totalAccepted.incrementAndGet();
						final FrontendReadListener readListener = new FrontendReadListener();
						accepted.getSourceChannel().setReadListener(readListener);
						accepted.getSourceChannel().setCloseListener(readListener);
						readListener.streamConnection = accepted;
						sessionsCount.incrementAndGet();
						
						final FrontendWriteListener writeListener = new FrontendWriteListener(readListener);
						accepted.getSinkChannel().setWriteListener(writeListener);
						accepted.getSinkChannel().setCloseListener(writeListener);
						readListener.writeListener = writeListener;

						accepted.getSourceChannel().resumeReads();
						//accepted.getSinkChannel().resumeWrites();
						readListeners.push(readListener);
					}
				} catch (IOException e) {
					log.error("", e);
					try {
						if(isDebug)log.debug("Closing channel: "+channel);
						channel.close();
					} catch (IOException e1) {
						log.error("", e1);
					}
				}
			}
		};
		return acceptListener2;
	}

	private final static class FrontendReadListener implements ChannelListener<ConduitStreamSourceChannel> {
		private ByteBuffer buffer = pool.allocate();
		private long lastActivity = System.currentTimeMillis();
		
		private volatile boolean writeHeaders=false;
		
		private Request8 request=null;
		private volatile boolean allClosed=false;
		private StreamConnection streamConnection;
		private FrontendWriteListener writeListener;
		private long totalWritesToFrontend=0;
		private long totalReadsFromFrontend=0;

		private FrontendReadListener(){
			buffer.clear();
		}
		
		@Override
		public final void handleEvent(final ConduitStreamSourceChannel frontendChannel) {
			frontendChannel.suspendReads();
			if(isDebug)MDC.put("channel", streamConnection.hashCode());
			if(!streamConnection.isOpen()|| !frontendChannel.isOpen()){
				if(isDebug)log.debug("Frontend channel is closed.");
				ByteBufferPool.free(buffer);
				closeAll();
				return;
			}
			
			try {
				try{
					buffer.clear();
					final int clientReadBytes = frontendChannel.read(buffer);
					if(clientReadBytes == -1){
						if(isDebug)log.debug("Client End of stream.");
						closeAll();
						return;
					}
					if(isDebug)log.debug("Read "+clientReadBytes+" bytes from frontend (source)");
					globalClientReadBytes.addAndGet(clientReadBytes);
					totalReadsFromFrontend += clientReadBytes;
					buffer.flip();
					
					if(request == null) {
						request = new Request8(buffer);
						writeListener.req = request;
					}else {
						request.parseRequest(buffer);
					}

					if(isDebug)log.debug(request);
					
					if(request.isMoreToRead()) {
						if(isDebug)log.debug("Resuming Reads");
						frontendChannel.resumeReads();
					} else {
						buffer.position(buffer.limit());
						if(isDebug)log.debug("Resuming Writes");
						streamConnection.getSinkChannel().resumeWrites();
					}
					
				}catch(IOException e){
					if("Connection reset by peer".equals(e.getMessage())){
						log.info("Connection reset by Frontend (source): "+((InetSocketAddress)streamConnection.getPeerAddress()).toString());
					}else{
						log.error("Error reading from Frontend (source): "+((InetSocketAddress)streamConnection.getPeerAddress()).toString(), e);
					}
					closeAll();
					return;
				}
				lastActivity = System.currentTimeMillis();
			} catch (Exception e) {
				log.error("", e);
				closeAll();
			} finally {
				if(isInfo)MDC.remove("channel");
			}
		}

		public final boolean checkLiveness() {
			if(!streamConnection.isOpen()){
				if(isDebug)log.debug("Frontend channel is closed.");
				closeAll();
				return false;
			}
			if(!streamConnection.getSinkChannel().isOpen()){
				if(isDebug)log.debug("Frontend sink channel is closed.");
				closeAll();
				return false;
			}
			if(!streamConnection.getSourceChannel().isOpen()){
				if(isDebug)log.debug("Frontend source channel is closed.");
				closeAll();
				return false;
			}
			return true;
		}

		private final void closeAll() {
			if(allClosed){
				return;
			}
			allClosed=true;
			sessionsCount.decrementAndGet();
			if(isDebug)log.debug("Closing all resources.");
			try{
				streamConnection.close();
				streamConnection.getSinkChannel().close();
			}catch(IOException e1){
				log.error("", e1);
			}finally{
				CustomByteBufferPool.free(buffer);
				this.buffer = null;
				this.writeListener.buffer = null;
				if(isInfo){
					String backendAddr = "";
					log.info(String.format("Session Closed. Frontend: "+((InetSocketAddress)streamConnection.getPeerAddress()).toString()+", Backend: "+backendAddr+" Stats: %,d reads from frontend, %,d writes to frontend", totalReadsFromFrontend, totalWritesToFrontend));
				}
			}
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("ReadListener [lastActivity=").append(lastActivity).append(", buffer=")
					.append(buffer).append(", writeHeaders=")
					.append(writeHeaders).append(", allClosed=").append(allClosed)
					.append(", streamConnection=").append(streamConnection).append(", writeListener=")
					.append(", totalWritesToFrontend=").append(totalWritesToFrontend)
					.append(", totalReadsFromFrontend=").append(totalReadsFromFrontend);
			builder.append(", frontent source resumed? ").append(streamConnection.getSinkChannel().isWriteResumed());
			builder.append(", frontent sink resumed? ").append(streamConnection.getSinkChannel().isWriteResumed());
			builder.append("]");
			return builder.toString();
		}
	}
	
	private final static class FrontendWriteListener implements ChannelListener<ConduitStreamSinkChannel> {
		private final FrontendReadListener readListener;		
		private Request8 req;
		private StreamConnection streamConnection;
		private ByteBuffer buffer;
		
		public FrontendWriteListener(FrontendReadListener readListener){
			this.readListener = readListener;
			this.streamConnection = readListener.streamConnection;
			this.buffer = readListener.buffer;
		}
		
		@Override
		public final void handleEvent(final ConduitStreamSinkChannel channel) {
			if(readListener.allClosed){
				return;
			}
			
			if(isInfo)MDC.put("channel", streamConnection.hashCode());
			
			if(!streamConnection.getSourceChannel().isOpen()){
				if(isDebug)log.debug("Frontend channel is closed.");
				readListener.closeAll();
				return;
			}else if(!channel.isOpen()){
				if(isDebug)log.debug("Frontent sink is closed.");
				readListener.closeAll();
				return;
			}
			
			//channel.suspendWrites();
			
			try {
				if(req.isWriteHeader()) {
					writeOKHeader(channel);
					if(readListener.allClosed) {
						return;
					}
				}
				int remaining = buffer.remaining();
				final int remainingToWrite = req.remainingWrites();
				if(remaining == 0 && remainingToWrite == 0 && !req.isKeepAlive()) {
						readListener.closeAll();
						return;
				}else if(remaining == 0 && remainingToWrite == 0) {
					if(isDebug)log.debug("Suspending writes");
					channel.suspendWrites();
					if(isDebug)log.debug("Resuming reads");
					streamConnection.getSourceChannel().resumeReads();
					return;
				}else if(remaining == 0 && remainingToWrite > 0) {
					buffer.clear();
					final int capacity = buffer.capacity();
					for(int i=0;i<capacity && i<remainingToWrite;i++) {
						buffer.put((byte) '0');
					}
					buffer.flip();
					remaining = buffer.remaining();
				}
				
				try{
					int count = channel.write(buffer);
					boolean flushed = channel.flush();
					req.incrementBodyBytesWritten(count);
					readListener.totalWritesToFrontend += count;
					globalClientWriteBytes.addAndGet(count);
					if(isDebug) {
						log.debug("Wrote "+count+" body bytes from backend to client (flushed: "+flushed+")");
						log.debug(req);
					}
					if(count != remaining){
						if(isDebug)log.debug("pending writes...");
						channel.resumeWrites();
						return;
					}else if(count == remaining && req.getBodyBytesWritten() == req.getContentLength()){
						if(isDebug)log.debug("Finished writing body.");
						req.reset();
						if(isDebug)log.debug("Suspending writes");
						channel.suspendWrites();
						if(isDebug)log.debug("Resuming reads.");
						streamConnection.getSourceChannel().resumeReads();
						return;
					}else if(!req.isKeepAlive() && remainingToWrite == 0) {
						readListener.closeAll();
						return;
					}else {
						if(isDebug)log.debug("Resuming reads.");
						streamConnection.getSourceChannel().resumeReads();
						return;
					}
				}catch(IOException e){
					log.error("Error writing to Frontend (sink) "+((InetSocketAddress)streamConnection.getPeerAddress()).toString(),e);
					readListener.closeAll();
					return;
				}
			} catch (Exception e) {
				if(!routerOptions.disableStacktraces) {
					log.error("", e);
				}
				readListener.closeAll();
				return;
			} finally {
				//if(isDebug)log.debug("Suspending writes on front-end (sink)");
				if(isInfo)MDC.remove("channel");
				//channel.suspendWrites();
			}
			
		}

		private void writeOKHeader(final ConduitStreamSinkChannel channel) throws IOException {
			if(readListener.request.getContentLength() != -1) {
				String ok_header = "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: "+readListener.request.getContentLength()+"\r\n\r\n";
				if(req.isKeepAlive()) {
					ok_header = "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: "+readListener.request.getContentLength()+"\r\n\r\n";
				}
				if("100-continue".equals(req.getExpect())) {
					ok_header = "HTTP/1.1 100 Continue\r\n\r\n";
				}
				
				final ByteBuffer okBuff = ByteBuffer.allocate(ok_header.getBytes().length);
				okBuff.put(ok_header.getBytes());
				okBuff.flip();
				int count = channel.write(okBuff);
				boolean flushed = channel.flush();
				if(isDebug)log.debug("Wrote "+count+" header bytes from backend to client (flushed: "+flushed+")");
				globalClientWriteBytes.addAndGet(count);
				req.setWriteHeader(false);
			}else {
				String ok_header = "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 5\r\n\r\nHello";
				if(req.isKeepAlive()) {
					ok_header = "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 5\r\n\r\nHello";
				}
				final ByteBuffer okBuff = ByteBuffer.allocate(ok_header.getBytes().length);
				okBuff.put(ok_header.getBytes());
				okBuff.flip();
				int count = channel.write(okBuff);
				boolean flushed = channel.flush();
				if(isDebug)log.debug("Wrote "+count+" header bytes from backend to client (flushed: "+flushed+")");
				req.incrementBodyBytesWritten(count);
				globalClientWriteBytes.addAndGet(count);
				if(!req.isKeepAlive()) {
					readListener.closeAll();
				}
			}
			readListener.streamConnection.getSourceChannel().resumeReads();
		}
	}

}
