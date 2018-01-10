import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * This is a Java implementation of the algorithm detailed in: 'A Fast, Minimal Memory, Consistent Hash Algorithm'
 * by John Lamping and Eric Veach at Google. It is also known as Jump Consistent Hashing. The algorithm can be used
 * to implement partitioning/hashing of data based on a hash (long).<br> 
 * 
 * The original algorithm was implemented in C++:<br>
 * <code>
 * <pre>
	int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets) {
		int64_t b = ­1, j = 0;
		while (j < num_buckets) {
			b = j;
			key = key * 2862933555777941757ULL + 1;
			j = (b + 1) * (double(1LL << 31) / double((key >> 33) + 1));
		}
		return b;
	}</pre></code>
 *
 * @authors John Lamping, Eric Veach @ Google
 *
 */
public class JumpConsistentHash {

	/**
	 * Test: Generate hashes for i from 0 to TOTAL.<br>
	 * Throughput is ~1 million hashes/sec on a Intel Dual Core i7@3.1 GHz 256 KB L2 cache 4MB L3 cache
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException{
		//Thread.sleep(30000);
		final int BUCKETS = 32;
		final long TOTAL = 1024l*1024;
		final int NUM_THREADS = 4;

		final Map<Integer, AtomicLong> counts = new ConcurrentHashMap<>(256);
		for(int i=0; i< BUCKETS; i++){
			counts.put(i, new AtomicLong(1));
		}
		final ForkJoinPool pool = new ForkJoinPool(4);
		final CountDownLatch latch = new CountDownLatch(NUM_THREADS);
		final Instant start = Instant.now();
		for(int i=0;i<NUM_THREADS;i++){
			pool.execute(new Runnable(){
				final SecureRandom random = new SecureRandom();
				@Override
				public void run() {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					for(long i=0l;i<TOTAL/NUM_THREADS;i++){
						long v = random.nextLong();
						int k = jumpConsistentHash(v, BUCKETS);
						counts.get(k).incrementAndGet();
					}
					latch.countDown();
				}

			});
		}
		latch.await();
		counts.forEach((a, b)->{
			System.out.println(a+" -> "+b);
		});
		System.out.println("--------------------");
		DoubleSummaryStatistics stats = counts.entrySet().stream().collect(Collectors.summarizingDouble(e -> e.getValue().get()));
		System.out.println(stats);
		Instant end = Instant.now();
		System.out.println(String.format("TOTAL: %,d", TOTAL));
		System.out.println(Duration.between(start, end));
		System.out.println(String.format("Rate: %,.0f per/sec", (double)TOTAL/((double)Duration.between(start, end).getSeconds())));
	}

	public static int jumpConsistentHash(long key, int buckets) {
		if (buckets < 0) {
			throw new IllegalArgumentException();
		}
		long k = key;
		long b = -1;
		long j = 0;

		while (j < buckets) {
			b = j;
			k = k * 2862933555777941757l + 1L;//i.e. 64-bit Linear Congruential Generator
			//j = (b + 1) * (double(1LL << 31) / double((key >> 33) + 1));
			j = (long) ((b + 1L) * (2147483648d / (double)((k >>> 33) + 1L)));//i.e. 2^31
		}
		return (int) b;
	}

	private static double toDouble(final long n) {
		double d = n & 9223372036854775807l;//i.e. 0x7fffffffffffffffL or (2^63) -1
		if (n < 0) {
			d += Long.MAX_VALUE;
		}
		return d;
	}
}
