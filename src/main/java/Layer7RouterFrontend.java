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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.xnio.ByteBufferPool;
import org.xnio.ChannelListener;
import org.xnio.CustomByteBufferPool;
import org.xnio.IoFuture;
import org.xnio.OptionMap;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
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
			.set(org.xnio.Options.BACKLOG, 1024 * 4)
			.set(org.xnio.Options.KEEP_ALIVE, false)
			.getMap();
	static XnioWorker worker;
	
	final static AtomicInteger totalAccepted = new AtomicInteger();
	final static AtomicInteger sessionsCount = new AtomicInteger();
	final static AtomicInteger sessionsActive = new AtomicInteger();

	final static AtomicLong globalClientWriteReq = new AtomicLong();
	final static AtomicLong globalClientWriteBytes = new AtomicLong();
	final static AtomicLong globalClientWriteRes = new AtomicLong();
	final static AtomicLong globalBackendReadBytes = new AtomicLong();

	
	final static AtomicInteger globalReqPerSec = new AtomicInteger();


	//static ByteBufferSlicePool pool = new ByteBufferSlicePool(1024*8, 32*1024*1024*32);
	static ByteBufferPool pool = CustomByteBufferPool.allocatePool(1024);
	
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
		long reqLast = globalClientWriteReq.get();

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
			double reqPerSec = ((double)globalClientWriteReq.get() - (double)reqLast) / ((double)slept/1000d);
			globalReqPerSec.set((int)((globalReqPerSec.get() * (1 - routerOptions.damping_factor)) + (reqPerSec * routerOptions.damping_factor)));
			
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
			final String formatted = String.format("Sess: %,.1f per/sec, %,d total, %,d curr, %,d active, %,d req, %,.1f req/sec, %,d res, %,d FW, %,d BR, out %,.1f %s/sec, in %,.1f %s/sec, Direct %,.1f MB, %s %s", 
					acceptedPerSec, totalAccepted.get(), sessionsCount.get(), sessionsActive.get(), globalClientWriteReq.get(), reqPerSec, globalClientWriteRes.get(), globalClientWriteBytes.get(), globalBackendReadBytes.get(), clientToBackendPerSec, clientToBackendPerSecUnits, backendToClientPerSec, backendToClientPerSecUnits, ((float)bufferPoolBean.getMemoryUsed())/(1024f*1024f), memoryStats, cpuStats);
			if(!formatted.equals(lastFormatted)){
				System.out.println(formatted);
			}
			lastFormatted = formatted;
			acceptedLast = totalAccepted.get();
			backendToClientLast = globalBackendReadBytes.get();
			clientToBackendLast = globalClientWriteBytes.get();
			reqLast = globalClientWriteReq.get();
			start = System.currentTimeMillis();
		}
	}

	private static void run() {
		final AtomicInteger connections= new AtomicInteger();
		final String backend_ok = "HTTP/1.1 200 OK\r\nContent-Length: "+routerOptions.payload_bytes+"\r\n\r\n";
		final StringBuilder sb = new StringBuilder();
		final String header = "GET / HTTP/1.1\r\nHost: "+routerOptions.backend_host+"\r\nConnection: keep-alive\r\nContent-Length: ";
		sb.append(header+(routerOptions.payload_bytes - (header.length() + 6))+"\r\n\r\n");
		while(sb.length() < routerOptions.payload_bytes) {
			sb.append("0");
		}

		final String req = sb.toString();
		//if(log.isDebugEnabled())log.debug("req: "+req.length()+":\n"+req);
		final InetSocketAddress backendAddr = new InetSocketAddress(routerOptions.backend_host, routerOptions.backend_port);
		final int total_conns = (routerOptions.client_end_ip-routerOptions.client_start_ip) * routerOptions.connections_per_ip;
		CountDownLatch latch = new CountDownLatch(total_conns);
		for(int ip=routerOptions.client_start_ip; ip<=routerOptions.client_end_ip;ip++) {
			for(int port=0; port<routerOptions.connections_per_ip;port++) {
				if(routerOptions.sleep_ms != null) {
					try{
						Thread.sleep(routerOptions.sleep_ms);
					}catch(Exception e){
						e.printStackTrace();
					}
				}
				
				final InetSocketAddress clientAddr = new InetSocketAddress("192.168.1."+ip, 0);
				//System.out.println(clientAddr.getAddress().getHostAddress()+":"+clientAddr.getPort()+" Connecting to "+backendAddr);
				final IoFuture<StreamConnection> future = worker.openStreamConnection(clientAddr, backendAddr, new ChannelListener<StreamConnection> () {
					@Override
					public void handleEvent(StreamConnection channel) {
						connections.incrementAndGet();
						totalAccepted.incrementAndGet();
						sessionsCount.incrementAndGet();
						latch.countDown();
						
						//System.out.println("Connections: "+connections.get());
						//System.out.println(addr+" Connected to "+backendAddr);
						channel.getSinkChannel().setWriteListener(new ChannelListener<ConduitStreamSinkChannel>(){
							final ByteBuffer buff = ByteBuffer.allocate(req.getBytes().length);
							volatile boolean remaining = false;
							@Override
							public void handleEvent(ConduitStreamSinkChannel c) {
								channel.getSourceChannel().suspendReads();
								c.suspendWrites();
								try {
									if(!remaining) {
										buff.put(req.getBytes());
										buff.flip();
										final String header = StandardCharsets.UTF_8.decode(buff).toString();
										buff.rewind();
										if(log.isDebugEnabled())log.debug("Writing Request: \n"+header);
										if(log.isDebugEnabled())log.debug(buff);
									}

									int pos = buff.position();
									int count = c.write(buff);
									boolean flushed = c.flush();
									buff.position(pos + count);
									if(log.isDebugEnabled())log.debug("Wrote "+count+" bytes. (flushed: "+flushed+")"+buff);
									if(buff.remaining() == 0) {
										c.suspendWrites();
										buff.clear();
										remaining = false;
										if(log.isDebugEnabled())log.debug("Finished sending request. Resuming Reads.");
										channel.getSourceChannel().resumeReads();
									}else {
										remaining = true;
										c.resumeWrites();
									}
									globalClientWriteBytes.addAndGet(count);
									globalClientWriteReq.incrementAndGet();
									
								} catch (IOException e) {
									e.printStackTrace();
									try {
										channel.close();
									} catch (IOException e1) {
										e1.printStackTrace();
									}
									
								}
							}
						});
						channel.getSourceChannel().setReadListener(new ChannelListener<ConduitStreamSourceChannel>(){
							private ByteBuffer readBuff = pool.allocate();
							int totalReadBodyBytes = 0;
							int contentLength = 0;
							{
								readBuff.clear();
							}
							@Override
							public void handleEvent(ConduitStreamSourceChannel c) {
								try {
									int count = c.read(readBuff);
									readBuff.flip();
									if(count == -1) {
										channel.close();
										return;
									}else if(count == 0) {
										return;
									}
									globalBackendReadBytes.addAndGet(count);
									if(log.isDebugEnabled())log.debug("Read "+count+" bytes from backend:");
									final String content = StandardCharsets.UTF_8.decode(readBuff).toString();
									if(log.isDebugEnabled())System.out.println(content);
									final String[] lines = content.toString().split("\r\n");
									if(lines.length > 0) {
										for(String line: lines){
											String[] header = line.split(":");
											if(header.length == 2) {
												if("Content-Length".equals(header[0])) {
													contentLength = Integer.parseInt(header[1].trim());
													String body = content.substring(content.lastIndexOf("\r\n\r\n")+4);
													if(log.isDebugEnabled())log.debug("body: "+body.length()+":\n"+body);
													totalReadBodyBytes += body.length();
													if(totalReadBodyBytes == contentLength) {
														globalClientWriteRes.incrementAndGet();
														totalReadBodyBytes = 0;
														c.suspendReads();
														readBuff.clear();
														if(log.isDebugEnabled())log.debug("Resuming client writes");
														channel.getSinkChannel().resumeWrites();
														return;
													}
												}
											}
										}
									}
									totalReadBodyBytes += count;
									if(totalReadBodyBytes >= contentLength) {
										globalClientWriteRes.incrementAndGet();
										totalReadBodyBytes = 0;
										c.suspendReads();
										readBuff.clear();
										if(log.isDebugEnabled())log.debug("Resuming client writes");
										channel.getSinkChannel().resumeWrites();
										return;
									}else {
										readBuff.clear();
										if(log.isDebugEnabled())log.debug("Resuming client reads");
										c.resumeReads();
									}

								} catch (IOException e) {
									e.printStackTrace();
									try {
										channel.close();
									} catch (IOException e1) {
										e1.printStackTrace();
									}
								}
							}
						});
						channel.setCloseListener(c->{
							
						});
						channel.getSourceChannel().resumeReads();
						channel.getSinkChannel().resumeWrites();
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

		try {
			latch.await();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		//regulate();
	}

	private static void regulate() {
		int maxReqPerSec = globalReqPerSec.get();
		int reqPerSecLast = globalReqPerSec.get();//damped avg
		double max_target_util=routerOptions.target_util;
		while(true) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			int count=0;
			if(globalReqPerSec.get() > maxReqPerSec) {
				maxReqPerSec = globalReqPerSec.get();
				max_target_util=routerOptions.target_util;
			}
			final Iterator<IoFuture<StreamConnection>> iter = futures.iterator();
			if(globalReqPerSec.get() < max_target_util){
				routerOptions.target_util = max_target_util;
			}else if(globalReqPerSec.get() >= reqPerSecLast) {
				routerOptions.target_util += 0.001d;
			}else {
				routerOptions.target_util -= 0.0015d;
			}
			if(routerOptions.target_util <= 0.001) {
				routerOptions.target_util = 0.001;
			}
			double currSessionsActive = ((double)sessionsCount.get()) * routerOptions.target_util;
			sessionsActive.set((int)currSessionsActive);
			double r = ((double)sessionsCount.get())/currSessionsActive;
			while(iter.hasNext()) {
				final IoFuture<StreamConnection> fut = iter.next();
				if(IoFuture.Status.DONE.equals(fut.getStatus())){
					try {
						if(routerOptions.sleep_ms != null) {
							Thread.sleep(routerOptions.sleep_ms);
						}
						if(((double)count++) % r < 1d) {
							fut.get().getSinkChannel().resumeWrites();
						}else {
							fut.get().getSinkChannel().suspendWrites();
						}
						reqPerSecLast = globalReqPerSec.get();
					} catch (CancellationException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			
			//System.out.println("target_util: "+target_util+" globalReqPerSec:"+globalReqPerSec.get()+" reqPerSecLast: "+reqPerSecLast+" sessionsActive:"+currSessionsActive+" r:"+r);
		}
	}
	
	private final static class Options {
		@Option(name = "-listen_port", usage="port")
		public Integer listen_port = 7080;
		
		@Option(name = "-backend_host", usage="ip address")
		public String backend_host = "192.168.1.150";
		
		@Option(name = "-backend_port", usage="port")
		public Integer backend_port = 80;
		
		@Option(name = "-client_start_ip", usage="ip address")
		public Integer client_start_ip = 80;
		
		@Option(name = "-client_end_ip", usage="ip address")
		public Integer client_end_ip = 120;
		
		@Option(name = "-sleep_ms", usage="sleep ms")
		public Integer sleep_ms = null;
		
		@Option(name = "-connections_per_ip", usage="connections per ip (int)")
		public Integer connections_per_ip = 20000;
		
		@Option(name = "-payload_bytes", usage="payload bytes, use powers of 2, no bigger than 32768 (int)")
		public Integer payload_bytes = 1024;
		
		@Option(name = "-damping_factor", usage="damping factor  (double between 0.01 and 1.0)")
		public Double damping_factor = 0.1d;
		
		@Option(name = "-target_util", usage="target utilization  (double between 0.01 and 1.0)")
		public Double target_util = 0.1d;

		@Override
		public String toString() {
			return "Options [listen_port=" + listen_port + ", backend_host=" + backend_host + ", backend_port="
					+ backend_port + ", client_start_ip=" + client_start_ip + ", client_end_ip=" + client_end_ip
					+ ", sleep_ms=" + sleep_ms + ", connections_per_ip=" + connections_per_ip + ", payload_bytes="
					+ payload_bytes + ", damping_factor=" + damping_factor + "]";
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
