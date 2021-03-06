import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import org.apache.log4j.MDC;
import org.kohsuke.args4j.CmdLineParser;
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

import com.boxymoron.request.Request8;

/**
 * TODO: Look into cpu/thread affinity.
 * @author royer
 *
 */
public final class Layer7RouterFrontend extends Common {
	
	final static Logger log = Logger.getLogger(Layer7RouterFrontend.class);
	
	final static boolean isInfo=log.isInfoEnabled();
	final static boolean isDebug=log.isDebugEnabled();
	final static boolean isTrace=log.isTraceEnabled();
	
	final static AtomicInteger totalAccepted = new AtomicInteger();
	final static AtomicInteger sessionsCount = new AtomicInteger();
	final static AtomicInteger sessionsActive = new AtomicInteger();
	final static AtomicLong globalClientWriteReq = new AtomicLong();
	final static AtomicLong globalClientWriteBytes = new AtomicLong();
	final static AtomicLong globalClientWriteRes = new AtomicLong();
	final static AtomicLong globalBackendReadBytes = new AtomicLong();
	final static AtomicInteger globalReqPerSec = new AtomicInteger();

	final static Xnio xnio = Xnio.getInstance();
	static XnioWorker worker;
	static OptionMap xnioOptions;
	//static ByteBufferSlicePool pool = new ByteBufferSlicePool(1024*8, 32*1024*1024*32);
	static ByteBufferPool pool;
	
	final static Options routerOptions = new Options();

	final static Deque<IoFuture<StreamConnection>> futures = new ConcurrentLinkedDeque<>();

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
				.set(org.xnio.Options.BACKLOG, routerOptions.backlog)
				.set(org.xnio.Options.KEEP_ALIVE, false)
				.getMap();

		worker = xnio.createWorker(xnioOptions);
		
		final Thread reaper = new Thread(new Runnable() {
			@Override
			public void run() {
				while(true) {
					try {
						Thread.sleep(2000);
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
		final byte[] req_arr = getReqBytes();
		//if(isDebug)log.debug("req: "+req.length()+":\n"+req);
		final InetSocketAddress backendAddr = new InetSocketAddress(routerOptions.backend_host, routerOptions.backend_port);
		final int total_conns = (routerOptions.client_end_ip-routerOptions.client_start_ip) * routerOptions.connections_per_ip;
		final CountDownLatch latch = new CountDownLatch(total_conns);
		for(int ip=routerOptions.client_start_ip; ip<=routerOptions.client_end_ip;ip++) {
			for(int port=0; port<routerOptions.connections_per_ip;port++) {
				if(routerOptions.sleep_ms != null) {
					try{
						Thread.sleep(routerOptions.sleep_ms);
					}catch(Exception e){
						e.printStackTrace();
					}
				}
				
				final InetSocketAddress clientAddr = new InetSocketAddress(routerOptions.client_base_ip+"."+ip, 0);
				//System.out.println(clientAddr.getAddress().getHostAddress()+":"+clientAddr.getPort()+" Connecting to "+backendAddr);
				final IoFuture<StreamConnection> future = worker.openStreamConnection(clientAddr, backendAddr, new ChannelListener<StreamConnection> () {
					volatile boolean done = false;
					@Override
					public void handleEvent(StreamConnection channel) {
						connections.incrementAndGet();
						totalAccepted.incrementAndGet();
						sessionsCount.incrementAndGet();
						latch.countDown();
						
						//System.out.println("Connections: "+connections.get());
						//System.out.println(addr+" Connected to "+backendAddr);
						channel.getSinkChannel().setWriteListener(new ChannelListener<ConduitStreamSinkChannel>(){
							final ByteBuffer buff = ByteBuffer.allocateDirect(req_arr.length);
							volatile boolean writesRemaining = false;
							volatile boolean doneWriting = false;
							{
								buff.put(req_arr);
							}
							@Override
							public void handleEvent(ConduitStreamSinkChannel c) {
								if(done || doneWriting) {
									return;
								}
								if(!channel.isOpen() || !c.isOpen()) {
									if(isDebug)log.debug("Connection is closed.");
									try {
										channel.close();
									} catch (IOException e) {
										if(!routerOptions.disableStacktraces) {
											e.printStackTrace();
										}
									}
									return;
								}
								if(isInfo)MDC.put("channel", channel.hashCode());
								try {
									if(!writesRemaining) {
										buff.clear();
										if(isDebug) {
											log.debug("Putting into buff: "+buff);
											log.debug(new String(req_arr));
										}
										buff.rewind();
										if(isDebug) {
											final String header = StandardCharsets.UTF_8.decode(buff).toString();
											buff.rewind();
											log.debug("Writing Request: \n"+header);
											log.debug(buff);
										}
									}

									final int remaining = buff.remaining();
									final int pos = buff.position();
									if(isDebug) {
										log.debug("Writing buff: "+buff);
									}
									final int count = c.write(buff);
									boolean flushed = false;
									if(routerOptions.flush) {
										flushed = c.flush();
									}
									buff.position(pos + count);
									if(isDebug)log.debug("Wrote "+count+" bytes. (flushed: "+flushed+")"+buff);
									if(buff.remaining() == 0) {
										c.suspendWrites();
										writesRemaining = false;
										if(isDebug)log.debug("Finished sending request. Resuming Reads.");
										if(!routerOptions.keepalive) {
											doneWriting = true;
											c.flush();
											c.close();
										}
										channel.getSourceChannel().resumeReads();
									}else {
										writesRemaining = true;
									}
									globalClientWriteBytes.addAndGet(count);
									globalClientWriteReq.incrementAndGet();
								} catch (IOException e) {
									if(!routerOptions.disableStacktraces) {
										e.printStackTrace();
									}
									try {
										channel.close();
									} catch (IOException e1) {
										if(!routerOptions.disableStacktraces) {
											e1.printStackTrace();
										}
									}
									if(isInfo)MDC.remove("channel");
								}
							}
						});
						channel.getSourceChannel().setReadListener(new ChannelListener<ConduitStreamSourceChannel>(){
							private ByteBuffer readBuff = pool.allocate();
							private int totalReadBodyBytes = 0;
							private int contentLength = 0;
							private Request8 req;
							{
								readBuff.clear();
							}
							@Override
							public void handleEvent(ConduitStreamSourceChannel c) {
								if(done) {
									return;
								}
								if(!channel.isOpen() || !c.isOpen()) {
									try {
										channel.close();
									} catch (IOException e) {
										if(!routerOptions.disableStacktraces) {
											e.printStackTrace();
										}
									}
									return;
								}
								if(isInfo)MDC.put("channel", channel.hashCode());
								try {
									int count = c.read(readBuff);
									readBuff.flip();
									if(count == -1) {
										System.out.println("count == -1");
										done = true;
										channel.close();
										CustomByteBufferPool.free(readBuff);
										return;
									}else if(count == 0) {
										return;
									}
									globalBackendReadBytes.addAndGet(count);
									if(isDebug)log.debug("Read "+count+" bytes from backend");

									if(isDebug) {
										log.debug("Before parse: "+req);
									}

									if(req == null) {
										req = new Request8(readBuff);
									}else {
										req.parseRequest(readBuff);
									}

									if(isDebug) {
										log.debug("After parse: "+req);
									}

									if(req.getContentLength() > -1) {
										readBuff.clear();
										if(!req.isMoreToRead()) {
											globalClientWriteRes.incrementAndGet();
											req.reset();
											c.suspendReads();
											if(isDebug)log.debug("Resuming client writes");
											channel.getSinkChannel().resumeWrites();
											return;
										}
									}else {
										readBuff.clear();
										totalReadBodyBytes += count;
										if(totalReadBodyBytes >= contentLength) {
											globalClientWriteRes.incrementAndGet();
											c.suspendReads();
											if(isDebug)log.debug("Resuming client writes");
											channel.getSinkChannel().resumeWrites();
											return;
										}
									}
								} catch (IOException e) {
									if(!routerOptions.disableStacktraces) {
										e.printStackTrace();
									}
									try {
										channel.close();
									} catch (IOException e1) {
										if(!routerOptions.disableStacktraces) {
											e1.printStackTrace();
										}
									}
									if(isInfo)MDC.remove("channel");
								}
							}
						});
						if(!routerOptions.regulate) {
							channel.getSinkChannel().resumeWrites();
						}
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
		
		if(routerOptions.regulate) {
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			regulate();
		}
	}

	private static byte[] getReqBytes() {
		final StringBuilder sb = new StringBuilder();
		final String header = "GET / HTTP/1.1\r\nHost: "+routerOptions.backend_host+"\r\nConnection: "+(routerOptions.keepalive ? "keep-alive" : "close")+"\r\nContent-Length: ";
		final int header_length = header.length();
		final int content_length = routerOptions.request_bytes-(header_length + 4);
		final String content_length_str = ""+content_length;
		sb.append(header).append(content_length-content_length_str.length()).append("\r\n\r\n");
		if(isDebug) {
			log.debug("Header length: "+(sb.length() - 4));
		}
		while(sb.length() < routerOptions.request_bytes) {
			sb.append("0");
		}
		if(isDebug) {
			log.debug("Total length (inc. boundary): "+(sb.length()));
			log.debug("boundary: "+sb.toString().indexOf("\r\n\r\n"));
		}

		final String req = sb.toString();
		final byte[] req_arr = req.getBytes();
		return req_arr;
	}

	private static void regulate() {
		int reqPerSecLast = globalReqPerSec.get();//damped avg
		while(true) {
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if(globalReqPerSec.get() >= reqPerSecLast) {
				routerOptions.target_util += 0.001d;
			}else {
				routerOptions.target_util -= 0.0015d;
			}
			if(routerOptions.target_util < 0.0001) {
				routerOptions.target_util = 0.0001;
			}
			double currSessionsActive = ((double)sessionsCount.get()) * routerOptions.target_util;
			sessionsActive.set((int)currSessionsActive);
			double r = ((double)sessionsCount.get())/currSessionsActive;
			final Iterator<IoFuture<StreamConnection>> iter = futures.iterator();
			int count=0;
			while(iter.hasNext()) {
				final IoFuture<StreamConnection> fut = iter.next();
				if(IoFuture.Status.DONE.equals(fut.getStatus())){
					try {
						if(((double)count++) % r < 1d) {
							fut.get().getSinkChannel().resumeWrites();
						}else {
							fut.get().getSinkChannel().suspendWrites();
						}
						
					} catch (CancellationException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			reqPerSecLast = globalReqPerSec.get();
			//System.out.println("target_util: "+target_util+" globalReqPerSec:"+globalReqPerSec.get()+" reqPerSecLast: "+reqPerSecLast+" sessionsActive:"+currSessionsActive+" r:"+r);
		}
	}

}
