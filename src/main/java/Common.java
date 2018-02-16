import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import org.kohsuke.args4j.Option;

public class Common {
	
	final private static int MB = 1024*1024;
	
	protected final static class Options {
		@Option(name = "-listen_port", usage="port")
		public Integer listen_port = 7080;
		
		@Option(name = "-backend_host", usage="host")
		public String backend_host = "192.168.1.150";
		
		@Option(name = "-backend_port", usage="port")
		public Integer backend_port = 80;
		
		@Option(name = "-client_base_ip", usage="first three octets of ip address")
		public String client_base_ip = "127.0.0";
		
		@Option(name = "-client_start_ip", usage="last ip address octet (int between 1 and 255)")
		public Integer client_start_ip = 1;
		
		@Option(name = "-client_end_ip", usage="last ip address octet (int between 1 and 255)")
		public Integer client_end_ip = 1;
		
		@Option(name = "-sleep_ms", usage="sleep ms")
		public Integer sleep_ms = null;
		
		@Option(name = "-connections_per_ip", usage="connections per ip (int)")
		public Integer connections_per_ip = 20000;
		
		@Option(name = "-damping_factor", usage="damping factor  (double between 0.01 and 1.0)")
		public Double damping_factor = 0.1d;
		
		@Option(name = "-target_util", usage="target utilization  (double between 0.01 and 1.0)")
		public Double target_util = 0.1d;
		
		@Option(name = "-keepalive", usage="use keep-alive  (boolean)")
		public boolean keepalive = false;
		
		@Option(name = "-buffer_size", usage="use powers of two (default 1024)")
		public int buffer_size = 1024;
		
		@Option(name = "-request_bytes", usage="request size in bytes (int) (defaults to -buffer_size)")
		public Integer request_bytes = buffer_size;
		
		@Option(name = "-num_threads", aliases={"-t"}, usage="number of worker threads (default 2)")
		public int num_threads = 2;
		
		@Option(name = "-regulate", usage="regulate front-end throughput (boolean default false)")
		public boolean regulate = false;
		
		@Option(name = "-disableStacktraces", usage="disable stracktraces (boolean default false)")
		public boolean disableStacktraces = false;
		
		@Option(name = "-flush", usage="flush writes (boolean default false)")
		public boolean flush = false;
		
		@Option(name = "-backlog", usage="backlog (default 64K)")
		public int backlog = 1024 * 64;

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Options [listen_port=").append(listen_port).append(", backend_host=").append(backend_host)
					.append(", backend_port=").append(backend_port).append(", client_base_ip=").append(client_base_ip)
					.append(", client_start_ip=").append(client_start_ip).append(", client_end_ip=")
					.append(client_end_ip).append(", sleep_ms=").append(sleep_ms).append(", connections_per_ip=")
					.append(connections_per_ip).append(", request_bytes=").append(request_bytes)
					.append(", damping_factor=").append(damping_factor).append(", target_util=").append(target_util)
					.append(", keepalive=").append(keepalive).append(", buffer_size=").append(buffer_size).append(", num_threads=").append(num_threads)
					.append(", regulate=").append(regulate).append(", disableStacktraces=").append(disableStacktraces).append(", flush=").append(flush)
					.append(", backlog=").append(backlog)
					.append("]");
			return builder.toString();
		}
	}
	
	protected static ThreadMXBean getWorkerCpuTimes(final Map<Long, Long> workerCpuTimes) {
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
	
	protected static StringBuilder getMemoryStats(Runtime runtime, MemoryMXBean mmxb) {
		StringBuilder memoryStats = new StringBuilder();
		memoryStats.append("Heap ").append(String.format("%04d", mmxb.getHeapMemoryUsage().getMax()/MB))
		.append("/").append(String.format("%04d", runtime.totalMemory()/MB))
		.append("/").append(String.format("%04d", mmxb.getHeapMemoryUsage().getUsed()/MB))
		.append("/").append(String.format("%04d", runtime.freeMemory()/MB)).append(" MB (M/T/U/F)");
		return memoryStats;
	}

	protected static StringBuilder getCpuStats(ThreadMXBean tmxb, final Map<Long, Long> workerCpuTimes, final double deltaSeconds) {
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
			}else if(currThread.getName().matches("(?i).*?(reference|finalizer|dispatcher|reap|cleaner).*")) {
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