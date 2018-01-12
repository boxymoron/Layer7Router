import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
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

	final static Xnio xnio = Xnio.getInstance();
	final static OptionMap xnioOptions = OptionMap.builder()
			.set(org.xnio.Options.ALLOW_BLOCKING, false)
			.set(org.xnio.Options.RECEIVE_BUFFER, 1024*4)
			.set(org.xnio.Options.SEND_BUFFER, 1024*4)
			//.set(org.xnio.Options.READ_TIMEOUT, 30000)
			//.set(org.xnio.Options.WRITE_TIMEOUT, 30000)
			.set(org.xnio.Options.USE_DIRECT_BUFFERS, true)
			.set(org.xnio.Options.WORKER_IO_THREADS, 2)
			.set(org.xnio.Options.SPLIT_READ_WRITE_THREADS, false)
			.set(org.xnio.Options.BACKLOG, 8192*2)
			.set(org.xnio.Options.KEEP_ALIVE, false)
			.getMap();
	static XnioWorker worker;
	
	final static AtomicInteger totalAccepted = new AtomicInteger();
	final static AtomicInteger sessionsCount = new AtomicInteger();

	final static AtomicLong globalClientWriteBytes = new AtomicLong();
	final static AtomicLong globalBackendReadBytes = new AtomicLong();


	//static ByteBufferSlicePool pool = new ByteBufferSlicePool(1024*8, 32*1024*1024*32);
	final static ByteBufferPool pool = CustomByteBufferPool.allocatePool(4096);
	
	final static Options routerOptions = new Options();
	
	final static boolean isInfo=log.isInfoEnabled();
	final static boolean isDebug=log.isDebugEnabled();
	final static boolean isTrace=log.isTraceEnabled();

	public static void main(String[] args) throws Exception {
		final CmdLineParser cmdLineParser = new CmdLineParser(routerOptions);
		cmdLineParser.parseArgument(args);
		System.out.println(routerOptions.toString());

		worker = xnio.createWorker(xnioOptions);
		
		ForkJoinPool.commonPool().execute(()->{
			run();
		});
		
		final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
		final Runtime runtime = Runtime.getRuntime();
		MemoryPoolMXBean edenBean = ManagementFactory.getMemoryPoolMXBeans().stream().filter(b->"PS Eden Space".equals(b.getName())).findFirst().get();
		BufferPoolMXBean bufferPoolBean = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream().filter(bb->"direct".equals(bb.getName())).findFirst().get();
		
		int acceptedLast = totalAccepted.get();

		long backendToClientLast = globalBackendReadBytes.get();
		long clientToBackendLast = globalClientWriteBytes.get();
		long start = System.currentTimeMillis();
		String lastFormatted="";
		while(true){
			Thread.sleep(2000);
			long slept = System.currentTimeMillis() - start;
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
			final String formatted = String.format("Sess: %,.1f per/sec, %,d total, %,d curr, %,d FW, %,d BR, out %,.1f %s/sec, in %,.1f %s/sec, Load %,.2f, HeapFree %,.1f MB, EdenUsed %,.1f MB, Direct %,.1f MB", 
					acceptedPerSec, totalAccepted.get(), sessionsCount.get(), globalClientWriteBytes.get(), globalBackendReadBytes.get(), clientToBackendPerSec, clientToBackendPerSecUnits, backendToClientPerSec, backendToClientPerSecUnits, operatingSystemMXBean.getSystemLoadAverage(), ((float)runtime.freeMemory())/(1024f*1024f), ((float)edenBean.getUsage().getUsed())/(1024f*1024f), ((float)bufferPoolBean.getMemoryUsed())/(1024f*1024f));
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
		final Deque<IoFuture<StreamConnection>> futures = new ConcurrentLinkedDeque<>();
		final InetSocketAddress backendAddr = new InetSocketAddress(routerOptions.backend_host, routerOptions.backend_port);
		for(int ip=routerOptions.client_start_ip; ip<=routerOptions.client_end_ip;ip++) {
			for(int port=0; port<40000;port++) {
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
							sessionsCount.decrementAndGet();
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
		public Integer client_end_ip = 255;

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Options: router_port=").append(listen_port).append(", backend_host=").append(backend_host)
					.append(", backend_port=").append(backend_port).append("");
			return builder.toString();
		}
	}

}
