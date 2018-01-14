import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.xnio.ByteBufferPool;
import org.xnio.ChannelListener;
import org.xnio.CustomByteBufferPool;
import org.xnio.IoFuture;
import org.xnio.IoFuture.Status;
import org.xnio.LocalSocketAddress;
import org.xnio.OptionMap;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.BoundChannel;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;

/**
 * TODO: Look into cpu/thread affinity.
 * @author royer
 *
 */
public final class Layer7RouterFrontend {

	final static Logger log = Logger.getLogger(Layer7RouterFrontend.class);
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
			.set(org.xnio.Options.BACKLOG, 1024)
			.set(org.xnio.Options.KEEP_ALIVE, false)
			.getMap();
	static XnioWorker worker;
	
	final static AtomicInteger totalAccepted = new AtomicInteger();
	final static AtomicInteger sessionsCount = new AtomicInteger();

	final static AtomicLong globalClientWriteBytes = new AtomicLong();
	final static AtomicLong globalBackendReadBytes = new AtomicLong();


	//static ByteBufferSlicePool pool = new ByteBufferSlicePool(1024*8, 32*1024*1024*32);
	final static ByteBufferPool pool = CustomByteBufferPool.allocatePool(1024);
	
	final static Options routerOptions = new Options();
	
	final static boolean isInfo=log.isInfoEnabled();
	final static boolean isDebug=log.isDebugEnabled();
	final static boolean isTrace=log.isTraceEnabled();
	
	final static Deque<IoFuture<StreamConnection>> futures = new ConcurrentLinkedDeque<>();

	public static void main(String[] args) throws Exception {
		final CmdLineParser cmdLineParser = new CmdLineParser(routerOptions);
		cmdLineParser.parseArgument(args);
		System.out.println(routerOptions.toString());

		worker = xnio.createWorker(xnioOptions);
		
		final Thread reaper = new Thread(new Runnable() {
			@Override
			public void run() {
				while(true) {
					try {
						Thread.sleep(500);
						final Iterator<IoFuture<StreamConnection>> iter = futures.iterator();
						while(iter.hasNext()) {
							final IoFuture<StreamConnection> fut = iter.next();
							if(IoFuture.Status.CANCELLED.equals(fut.getStatus()) || IoFuture.Status.FAILED.equals(fut.getStatus())) {
								sessionsCount.decrementAndGet();
								iter.remove();
								continue;
							}else if(IoFuture.Status.DONE.equals(fut.getStatus())) {
								if(!fut.get().isOpen() || !fut.get().getSinkChannel().isOpen() || !fut.get().getSourceChannel().isOpen()) {
									sessionsCount.decrementAndGet();
									iter.remove();
									continue;
								}
							}
						}
					}catch(Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
		
		reaper.setName("Idle Connection Reaper");
		reaper.start();
		
		ForkJoinPool.commonPool().execute(()->{
			run();
		});
		
		final Map <Long, Long> workerCpuTimes = new LinkedHashMap<Long, Long>();
		final Runtime runtime = Runtime.getRuntime();
		final BufferPoolMXBean bufferPoolBean = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream().filter(bb->"direct".equals(bb.getName())).findFirst().get();
		final MemoryMXBean mmxb = ManagementFactory.getMemoryMXBean();
		final ThreadMXBean tmxb = getWorkerCpuTimes(workerCpuTimes);
		
		int acceptedLast = totalAccepted.get();

		long backendToClientLast = globalBackendReadBytes.get();
		long clientToBackendLast = globalClientWriteBytes.get();
		long start = System.currentTimeMillis();
		String lastFormatted="";
		while(true){
			Thread.sleep(2000);
			long slept = System.currentTimeMillis() - start;
			
			final StringBuilder cpuStats = getCpuStats(tmxb, workerCpuTimes, ((double)slept)/1000d);
			final StringBuilder memoryStats = getMemoryStats(runtime, mmxb);
			
			double acceptedPerSec = ((double)totalAccepted.get() - (double)acceptedLast) / ((double)slept/1000d);

			double clientToBackendPerSec = (globalClientWriteBytes.get() - clientToBackendLast) / ((double)slept/1000d);
			String clientToBackendPerSecUnits = "KB";
			if(clientToBackendPerSec > ((1024*1024)-1)) {
				clientToBackendPerSecUnits = "MB";
				clientToBackendPerSec = clientToBackendPerSec / (1024f*1024f);
			}else {
				clientToBackendPerSec = clientToBackendPerSec / 1024f;
			}
			
			double backendToClientPerSec = (globalBackendReadBytes.get() - backendToClientLast) / ((double)slept/1000d);
			String backendToClientPerSecUnits = "KB";
			if(backendToClientPerSec > ((1024*1024)-1)) {
				backendToClientPerSecUnits = "MB";
				backendToClientPerSec = backendToClientPerSec / (1024f*1024f);
			}else {
				backendToClientPerSec = backendToClientPerSec / 1024f;
			}
			final String formatted = String.format("Sess: %,.1f per/sec, %,d total, %,d curr, %,d FW, %,d BR, out %,.1f %s/sec, in %,.1f %s/sec, Direct %,.1f MB, %s %s", 
					acceptedPerSec, totalAccepted.get(), sessionsCount.get(), globalClientWriteBytes.get(), globalBackendReadBytes.get(), clientToBackendPerSec, clientToBackendPerSecUnits, backendToClientPerSec, backendToClientPerSecUnits, ((float)bufferPoolBean.getMemoryUsed())/(1024f*1024f), memoryStats, cpuStats);
			if(!formatted.equals(lastFormatted)){
				System.out.println(formatted);
			}
			lastFormatted = formatted;
			acceptedLast = totalAccepted.get();
			backendToClientLast = globalBackendReadBytes.get();
			clientToBackendLast = globalClientWriteBytes.get();
			start = System.currentTimeMillis();
		}
	}

	private static void run() {
		final AtomicInteger connections= new AtomicInteger();
		
		final InetSocketAddress backendAddr = new InetSocketAddress(routerOptions.backend_host, routerOptions.backend_port);
		for(int ip=routerOptions.client_start_ip; ip<routerOptions.client_end_ip;ip++) {
			for(int port=0; port<20000;port++) {
				try{
					Thread.sleep(2);
				}catch(Exception e){
					e.printStackTrace();
				}
				final InetSocketAddress clientAddr = new InetSocketAddress("192.168.1."+ip, 0);
				//System.out.println(clientAddr.getAddress().getHostAddress()+":"+clientAddr.getPort()+" Connecting to "+backendAddr);
				final IoFuture<StreamConnection> future = worker.openStreamConnection(clientAddr, backendAddr, new ChannelListener<StreamConnection> () {
					@Override
					public void handleEvent(StreamConnection channel) {
						connections.incrementAndGet();
						totalAccepted.incrementAndGet();
						sessionsCount.incrementAndGet();
						//System.out.println("Connections: "+connections.get());
						ByteBuffer readBuff = pool.allocate();
						readBuff.clear();
						//System.out.println(addr+" Connected to "+backendAddr);
						channel.getSinkChannel().setWriteListener(c->{
							final String req = "GET /favicon.ico HTTP/1.1\r\nHost: "+routerOptions.backend_host+"\r\nConnection: keep-alive\r\n\r\n\r\n";
							final ByteBuffer buff = ByteBuffer.allocate(req.getBytes().length);
							buff.put(req.getBytes());
							buff.flip();
							try {
								int count = c.write(buff);
								globalClientWriteBytes.addAndGet(count);
								c.flush();
							} catch (IOException e) {
								e.printStackTrace();
							}
							c.suspendWrites();
						});
						channel.getSourceChannel().setReadListener(c->{
							try {
								int count = c.read(readBuff);
								if(count > 0) {
									globalBackendReadBytes.addAndGet(count);
									//System.out.println("read "+count+" bytes");
								}else {
									c.suspendReads();
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
						});
						channel.setCloseListener(c->{
							
						});
						channel.getSinkChannel().resumeWrites();
						channel.getSourceChannel().resumeReads();
					}}, new ChannelListener<BoundChannel>() {
						@Override
						public void handleEvent(BoundChannel channel) {
							//System.out.println("bound");
						}
					}, xnioOptions);
				futures.push(future);
				//Thread.sleep(1000);
			}
		}
	}
	
	private final static class Options {
		@Option(name = "-listen_port", usage="port")
		public Integer listen_port = 7080;
		
		@Option(name = "-backend_host", usage="host")
		public String backend_host = "192.168.1.150";
		
		@Option(name = "-backend_port", usage="port")
		public Integer backend_port = 80;
		
		@Option(name = "-client_start_ip", usage="port")
		public Integer client_start_ip = 225;
		
		@Option(name = "-client_end_ip", usage="port")
		public Integer client_end_ip = 235;

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Options: router_port=").append(listen_port).append(", backend_host=").append(backend_host)
					.append(", backend_port=").append(backend_port).append("");
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
