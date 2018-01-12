import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.xnio.ByteBufferPool;
import org.xnio.ChannelListener;
import org.xnio.OptionMap;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;

/**
 * TODO: Look into cpu/thread affinity.
 * @author royer
 *
 */
public final class Layer7RouterBackend {

	final static Logger log = Logger.getLogger(Layer7RouterBackend.class);

	final static Xnio xnio = Xnio.getInstance();
	final static OptionMap xnioOptions = OptionMap.builder()
			.set(org.xnio.Options.ALLOW_BLOCKING, false)
			.set(org.xnio.Options.RECEIVE_BUFFER, 1024*8)
			.set(org.xnio.Options.SEND_BUFFER, 1024*8)
			//.set(org.xnio.Options.READ_TIMEOUT, 30000)
			//.set(org.xnio.Options.WRITE_TIMEOUT, 30000)
			.set(org.xnio.Options.USE_DIRECT_BUFFERS, true)
			.set(org.xnio.Options.WORKER_IO_THREADS, 1)
			.set(org.xnio.Options.SPLIT_READ_WRITE_THREADS, false)
			.set(org.xnio.Options.BACKLOG, 8192)
			.set(org.xnio.Options.KEEP_ALIVE, false)
			.getMap();
	static XnioWorker worker;
	
	final static AtomicInteger totalAccepted = new AtomicInteger();
	final static AtomicInteger sessionsCount = new AtomicInteger();
	final static AtomicLong globalClientWriteBytes = new AtomicLong();
	final static AtomicLong globalClientReadBytes = new AtomicLong();

	//static ByteBufferSlicePool pool = new ByteBufferSlicePool(1024*8, 32*1024*1024*32);
	final static ByteBufferPool pool = ByteBufferPool.MEDIUM_DIRECT;
	
	final static Options routerOptions = new Options();
	
	final static boolean isInfo=log.isInfoEnabled();
	final static boolean isDebug=log.isDebugEnabled();
	final static boolean isTrace=log.isTraceEnabled();

	public static void main(String[] args) throws Exception {
		final CmdLineParser cmdLineParser = new CmdLineParser(routerOptions);
		cmdLineParser.parseArgument(args);
		System.out.println(routerOptions.toString());

		worker = xnio.createWorker(xnioOptions);
		
		
		final Deque<FrontendReadListener> readListeners = new ConcurrentLinkedDeque<>();

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
						accepted.getSinkChannel().resumeWrites();
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

		final AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(new InetSocketAddress(routerOptions.listen_port), acceptListener2, OptionMap.EMPTY);
		server.resumeAccepts();

		if(isInfo)log.info("Listening on " + server.getLocalAddress());
		
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
		
		final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
		final Runtime runtime = Runtime.getRuntime();
		MemoryPoolMXBean edenBean = ManagementFactory.getMemoryPoolMXBeans().stream().filter(b->"PS Eden Space".equals(b.getName())).findFirst().get();
		BufferPoolMXBean bufferPoolBean = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream().filter(bb->"direct".equals(bb.getName())).findFirst().get();
		
		int acceptedLast = totalAccepted.get();
		long backendToClientLast = globalClientWriteBytes.get();
		long start = System.currentTimeMillis();
		String lastFormatted="";
		while(true){
			Thread.sleep(2000);
			long slept = System.currentTimeMillis() - start;
			double acceptedPerSec = ((double)totalAccepted.get() - (double)acceptedLast) / ((double)slept/1000d);
			double backendToClientPerSec = (globalClientWriteBytes.get() - backendToClientLast) / ((double)slept/1000d);
			String backendToClientPerSecUnits = "KB";
			if(backendToClientPerSec > ((1024*1024)-1)) {
				backendToClientPerSecUnits = "MB";
				backendToClientPerSec = backendToClientPerSec / (1024f*1024f);
			}else {
				backendToClientPerSec = backendToClientPerSec / 1024f;
			}
			final String formatted = String.format("Sess: %,.1f per/sec, %,d total, %,d curr, %,d FR, %,d FW, out %,.1f %s/sec, Load %,.2f, HeapFree %,.1f MB, EdenUsed %,.1f MB, Direct %,.1f MB", 
					acceptedPerSec, totalAccepted.get(), sessionsCount.get(), globalClientReadBytes.get(), globalClientWriteBytes.get(), backendToClientPerSec, backendToClientPerSecUnits, operatingSystemMXBean.getSystemLoadAverage(), ((float)runtime.freeMemory())/(1024f*1024f), ((float)edenBean.getUsage().getUsed())/(1024f*1024f), ((float)bufferPoolBean.getMemoryUsed())/(1024f*1024f));
			if(!formatted.equals(lastFormatted)){
				System.out.println(formatted);
			}
			lastFormatted = formatted;
			acceptedLast = totalAccepted.get();
			backendToClientLast = globalClientWriteBytes.get();
			start = System.currentTimeMillis();
		}
	}

	private final static class FrontendReadListener implements ChannelListener<ConduitStreamSourceChannel> {
		private final static Logger log = Logger.getLogger(FrontendReadListener.class);
		
		private long lastActivity = System.currentTimeMillis();
		private final ByteBuffer buffer = pool.allocate();

		private volatile boolean initHeaders=true;
		private volatile boolean writeHeaders=false;
		private volatile boolean isProxy=false;
		private volatile boolean writeSSLHeadersFromClient=false;

		private volatile boolean allClosed=false;
		private StreamConnection streamConnection;
		private FrontendWriteListener writeListener;
		private long totalWritesToBackend=0;
		private long totalWritesToFrontend=0;
		private long totalReadsFromBackend=0;
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
				closeAll();
				return;
			}
			
			try {
				long clientReadBytes=0;
				try{
					if(!initHeaders && buffer.hasRemaining() && !writeSSLHeadersFromClient){
						return;//the buffer still has stuff to write
					}

					buffer.clear();
					clientReadBytes = frontendChannel.read(buffer);
					if(clientReadBytes == -1){
						if(isDebug)log.debug("Client End of stream.");
						closeAll();
						return;
					}
					
					globalClientReadBytes.addAndGet(clientReadBytes);
					buffer.flip();
					this.streamConnection.getSinkChannel().resumeWrites();
					if(isDebug){
						totalReadsFromFrontend += clientReadBytes;
						log.debug(buffer.toString());
						log.debug("Read "+clientReadBytes+" bytes from frontend (source)");
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
			}catch(IOException e1){
				log.error("", e1);
			}finally{

				ByteBufferPool.free(buffer);

				if(isInfo){
					String backendAddr = "";
					
					log.info(String.format("Session Closed. Frontend: "+((InetSocketAddress)streamConnection.getPeerAddress()).toString()+", Backend: "+backendAddr+" Stats: %,d writes to backend, %,d reads from frontend, %,d writes to frontend, %,d reads from backend", totalWritesToBackend, totalReadsFromFrontend, totalWritesToFrontend, totalReadsFromBackend));
				}
			}
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("ReadListener [lastActivity=").append(lastActivity).append(", buffer=")
					.append(buffer).append(", initHeaders=").append(initHeaders).append(", writeHeaders=")
					.append(writeHeaders).append(", allClosed=").append(allClosed)
					.append(", streamConnection=").append(streamConnection).append(", writeListener=")
					.append(writeListener).append(", totalWritesToBackend=").append(totalWritesToBackend)
					.append(", totalWritesToFrontend=").append(totalWritesToFrontend).append(", totalReadsFromBackend=")
					.append(totalReadsFromBackend).append(", totalReadsFromFrontend=").append(totalReadsFromFrontend)
					.append(", isProxy=").append(isProxy);
			builder.append(", frontent source resumed? ").append(streamConnection.getSinkChannel().isWriteResumed());
			builder.append(", frontent sink resumed? ").append(streamConnection.getSinkChannel().isWriteResumed());
			builder.append("]");
			return builder.toString();
		}
	}
	
	

	private final static class FrontendWriteListener implements ChannelListener<ConduitStreamSinkChannel> {
		private final FrontendReadListener readListener;

		private boolean init=true;
		public FrontendWriteListener(FrontendReadListener readListener){
			this.readListener = readListener;
		}
		
		@Override
		public final void handleEvent(final ConduitStreamSinkChannel channel) {
			if(readListener.allClosed){
				return;
			}
			if(isInfo)MDC.put("channel", readListener.streamConnection.hashCode());
			if(!readListener.streamConnection.isOpen() || !channel.isOpen()){
				if(isDebug)log.debug("Frontend channel is closed.");
				readListener.closeAll();
				return;
			}
			if(!channel.isOpen()){
				if(isDebug)log.debug("Frontent sink is closed.");
				readListener.closeAll();
				return;
			}
			channel.suspendWrites();
			try {
				if(init) {
					final String PROXY_HEADER = "HTTP/1.1 200 OK\r\n\r\n";
					final ByteBuffer okBuff = ByteBuffer.allocate(PROXY_HEADER.getBytes().length);
					okBuff.put(PROXY_HEADER.getBytes());
					okBuff.flip();
					channel.write(okBuff);
				}
				int remaining = readListener.buffer.remaining();
				if(remaining == 0){
					return;
				}
				try{
					long count = channel.write(readListener.buffer);
					boolean flushed = channel.flush();
					if(isDebug)log.debug("Wrote "+count+" bytes from backend to client (flushed: "+flushed+")");
					readListener.totalWritesToFrontend += count;
					globalClientWriteBytes.addAndGet(count);
					if(count != remaining){
						channel.resumeWrites();
						return;
					}
				}catch(IOException e){
					log.error("Error writing to Frontend (sink) "+((InetSocketAddress)readListener.streamConnection.getPeerAddress()).toString(),e);
					readListener.closeAll();
					return;
				}
			} catch (Exception e) {
				log.error("", e);
				readListener.closeAll();
				return;
			} finally {
				//if(isDebug)log.debug("Suspending writes on front-end (sink)");
				if(isInfo)MDC.remove("channel");
				//channel.suspendWrites();
			}
			
		}
	}
	
	private final static class Options {
		@Option(name = "-listen_port", usage="port")
		public Integer listen_port = 7180;

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Options: router_port=").append(listen_port);
			return builder.toString();
		}
	}

}