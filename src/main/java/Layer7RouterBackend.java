import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
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
import org.kohsuke.args4j.Option;
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

/**
 * TODO: Look into cpu/thread affinity.
 * @author royer
 *
 */
public final class Layer7RouterBackend {

	final static Logger log = Logger.getLogger(Layer7RouterBackend.class);
	final private static int MB = 1024*1024;
	
	final static Xnio xnio = Xnio.getInstance();
	final static OptionMap xnioOptions = OptionMap.builder()
			.set(org.xnio.Options.ALLOW_BLOCKING, false)
			.set(org.xnio.Options.RECEIVE_BUFFER, 1024)
			.set(org.xnio.Options.SEND_BUFFER, 1024)
			//.set(org.xnio.Options.READ_TIMEOUT, 30000)
			//.set(org.xnio.Options.WRITE_TIMEOUT, 30000)
			.set(org.xnio.Options.USE_DIRECT_BUFFERS, true)
			.set(org.xnio.Options.WORKER_IO_THREADS, 2)
			.set(org.xnio.Options.SPLIT_READ_WRITE_THREADS, false)
			.set(org.xnio.Options.BACKLOG, 1024*4)
			.set(org.xnio.Options.KEEP_ALIVE, false)
			.getMap();
	static XnioWorker worker;
	
	final static AtomicInteger totalAccepted = new AtomicInteger();
	final static AtomicInteger sessionsCount = new AtomicInteger();
	final static AtomicLong globalClientWriteBytes = new AtomicLong();
	final static AtomicLong globalClientReadBytes = new AtomicLong();

	//static ByteBufferSlicePool pool = new ByteBufferSlicePool(1024*8, 32*1024*1024*32);
	final static ByteBufferPool pool = CustomByteBufferPool.allocatePool(1024);
	
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

	private final static class FrontendReadListener implements ChannelListener<ConduitStreamSourceChannel> {
		private final static Logger log = Logger.getLogger(FrontendReadListener.class);
		
		private long lastActivity = System.currentTimeMillis();
		private final ByteBuffer buffer = pool.allocate();

		private volatile boolean writeHeaders=false;
		
		private Request request=null;

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
					buffer.clear();
					clientReadBytes = frontendChannel.read(buffer);
					if(clientReadBytes == -1){
						if(isDebug)log.debug("Client End of stream.");
						closeAll();
						return;
					}
					globalClientReadBytes.addAndGet(clientReadBytes);
					buffer.flip();
					
					request = parseRequest();
					
					if(request.equals(Request.BODY)) {
						this.streamConnection.getSinkChannel().resumeWrites();
					}else {
						this.streamConnection.getSinkChannel().resumeWrites();
					}
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

		private Request parseRequest() {
			final String content = StandardCharsets.UTF_8.decode(buffer).toString();
			buffer.rewind();
			int boundary = content.indexOf("\r\n\r\n")+4;
			int content_length = buffer.limit() - boundary;
			String[] headersArray = content.substring(0, boundary).split("\r\n");
			LinkedHashMap<String, String> headers = new LinkedHashMap<>();
			for(String header : headersArray) {
				if(header.startsWith("GET") || header.startsWith("POST")) {
					headers.put("REQ", header);
					continue;
				}else {
					final String[] headerParts = header.split(": ");
					if(headerParts.length == 2) {
						headers.put(headerParts[0], headerParts[1]);
					}
				}
			}
			if(headers.isEmpty()) {
				return Request.BODY;
			}else {
				if(headers.containsKey("REQ")) {
					buffer.position(boundary);
					return new Request(headers, boundary);
				}else {
					return Request.BODY;
				}
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
					.append(buffer).append(", writeHeaders=")
					.append(writeHeaders).append(", allClosed=").append(allClosed)
					.append(", streamConnection=").append(streamConnection).append(", writeListener=")
					.append(writeListener).append(", totalWritesToBackend=").append(totalWritesToBackend)
					.append(", totalWritesToFrontend=").append(totalWritesToFrontend).append(", totalReadsFromBackend=")
					.append(totalReadsFromBackend).append(", totalReadsFromFrontend=").append(totalReadsFromFrontend);
			builder.append(", frontent source resumed? ").append(streamConnection.getSinkChannel().isWriteResumed());
			builder.append(", frontent sink resumed? ").append(streamConnection.getSinkChannel().isWriteResumed());
			builder.append("]");
			return builder.toString();
		}
	}

	private static class Request {
		final static Request BODY = new Request(null, -1);
		final LinkedHashMap<String, String> headers;
		final int boundary;
		public Request(LinkedHashMap<String, String> headers, int boundary) {
			this.headers = headers;
			this.boundary = boundary;
		}
	}
	
	private final static class FrontendWriteListener implements ChannelListener<ConduitStreamSinkChannel> {
		private final FrontendReadListener readListener;

		private boolean writeHeader=true;
		private boolean writeBody=false;

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
				if(Request.BODY != readListener.request) {
					writeOKHeader(channel);
					return;
				}
				int remaining = readListener.buffer.remaining();
				if(remaining == 0) {
					readListener.streamConnection.getSourceChannel().resumeReads();
					return;
				}
				try{
					long count = channel.write(readListener.buffer);
					boolean flushed = channel.flush();
					if(isDebug)log.debug("Wrote "+count+" body bytes from backend to client (flushed: "+flushed+")");
					readListener.totalWritesToFrontend += count;
					globalClientWriteBytes.addAndGet(count);
					if(count != remaining){
						channel.resumeWrites();
						return;
					}else {
						
						readListener.streamConnection.getSourceChannel().resumeReads();
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

		private void writeOKHeader(final ConduitStreamSinkChannel channel) throws IOException {
			if(readListener.request.headers.containsKey("Content-Length")) {
				int contentLength = Integer.parseInt(readListener.request.headers.get("Content-Length"));
				final String ok_header = "HTTP/1.1 200 OK\r\nContent-Length: "+contentLength+"\r\n\r\n";
				final ByteBuffer okBuff = ByteBuffer.allocate(ok_header.getBytes().length);
				okBuff.put(ok_header.getBytes());
				okBuff.flip();
				int count = channel.write(okBuff);
				boolean flushed = channel.flush();
				if(isDebug)log.debug("Wrote "+count+" header bytes from backend to client (flushed: "+flushed+")");
				globalClientWriteBytes.addAndGet(count);
				if(readListener.buffer.hasRemaining()) {
					count = channel.write(readListener.buffer);
					flushed = channel.flush();
					if(isDebug)log.debug("Wrote "+count+" body bytes from backend to client (flushed: "+flushed+")");
				}
				
			}else {
				String ok_header = "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 5\r\n\r\nHello";
				if("keep-alive".equals(readListener.request.headers.get("Connection"))) {
					ok_header = "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 5\r\n\r\nHello";
				}
				final ByteBuffer okBuff = ByteBuffer.allocate(ok_header.getBytes().length);
				okBuff.put(ok_header.getBytes());
				okBuff.flip();
				int count = channel.write(okBuff);
				boolean flushed = channel.flush();
				if(isDebug)log.debug("Wrote "+count+" header bytes from backend to client (flushed: "+flushed+")");
				globalClientWriteBytes.addAndGet(count);
				if(!"keep-alive".equals(readListener.request.headers.get("Connection"))) {
					readListener.closeAll();
				}
			}
			readListener.request = Request.BODY;
			readListener.streamConnection.getSourceChannel().resumeReads();
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
	
	private static ThreadMXBean getWorkerCpuTimes(final Map<Long, Long> workerCpuTimes) {
		ThreadMXBean tmxb = ManagementFactory.getThreadMXBean();
		tmxb.setThreadCpuTimeEnabled(true);
		
		workerCpuTimes.put(Thread.currentThread().getId(), tmxb.getThreadCpuTime(Thread.currentThread().getId()));//monitor thyself
		Thread[] threads = getAllThreads();
		Arrays.sort(threads, new Comparator<Thread>(){
			@Override
			public int compare(Thread o1, Thread o2) {
				return new Long(o1.getId()).compareTo(new Long(o2.getId()));
			}});
		for(Thread thread : threads){
			workerCpuTimes.put(thread.getId(), tmxb.getThreadCpuTime(thread.getId()));
		}
		return tmxb;
	}
	
	private static ThreadGroup rootThreadGroup = null;
	private final static ThreadGroup getRootThreadGroup( ) {
	    if ( rootThreadGroup != null )
	        return rootThreadGroup;
	    ThreadGroup tg = Thread.currentThread( ).getThreadGroup( );
	    ThreadGroup ptg;
	    while ( (ptg = tg.getParent( )) != null )
	        tg = ptg;
	    return tg;
	}
	
	private final static Thread[] getAllThreads( ) {
	    final ThreadGroup root = getRootThreadGroup( );
	    final ThreadMXBean thbean = ManagementFactory.getThreadMXBean( );
	    int nAlloc = thbean.getThreadCount( );
	    int n = 0;
	    Thread[] threads;
	    do {
	        nAlloc *= 2;
	        threads = new Thread[ nAlloc ];
	        n = root.enumerate( threads, true );
	    } while ( n == nAlloc );
	    return java.util.Arrays.copyOf( threads, n );
	}
	
	private static StringBuilder getMemoryStats(Runtime runtime, MemoryMXBean mmxb) {
		StringBuilder memoryStats = new StringBuilder();
		memoryStats.append("Heap ").append(String.format("%04d", mmxb.getHeapMemoryUsage().getMax()/MB))
		.append("/").append(String.format("%04d", runtime.totalMemory()/MB))
		.append("/").append(String.format("%04d", mmxb.getHeapMemoryUsage().getUsed()/MB))
		.append("/").append(String.format("%04d", runtime.freeMemory()/MB)).append(" MB (M/T/U/F)");
		return memoryStats;
	}

	private static StringBuilder getCpuStats(ThreadMXBean tmxb, final Map<Long, Long> workerCpuTimes, final double deltaSeconds) {
		StringBuilder threadStats = new StringBuilder();
		double totalCpuUsage = 0;
		for(Iterator<Long> iter = workerCpuTimes.keySet().iterator(); iter.hasNext();){
			Long threadId = iter.next();
			long previousCpuTimeNanos = workerCpuTimes.get(threadId);
			//System.out.println(previousCpuTimeNanos);
			long currCpuTimeNanos = tmxb.getThreadCpuTime(threadId);//this seems to have a precision of 10 uS
			double deltaCpuTimeNanos = currCpuTimeNanos - previousCpuTimeNanos;
			double cpuUsage = ((deltaCpuTimeNanos/(double)1000000000d)/deltaSeconds);
			totalCpuUsage+=cpuUsage;
			Thread currThread = getThread(threadId);
			if(currThread == null){
				iter.remove();
				continue;
			}else if(currThread.getName().matches("(?i).*?(reference|finalizer|dispatcher|reap).*")) {
				iter.remove();
				continue;
			}
			threadStats.append(" | ").append(currThread.getName()).append(" ")
			.append(String.format("%03.1f", cpuUsage*(double)100)).append("%");
			workerCpuTimes.put(threadId, currCpuTimeNanos);
		}
		threadStats.append(" | Cpu Total ").append(String.format("%03.1f", totalCpuUsage*(double)100)).append("%");
		return threadStats;
	}
	
	final private static Thread getThread( final long id ) {
	    final Thread[] threads = getAllThreads( );
	    for ( Thread thread : threads )
	        if ( thread.getId( ) == id )
	            return thread;
	    return null;
	}

}
