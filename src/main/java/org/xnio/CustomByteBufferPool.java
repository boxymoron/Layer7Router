package org.xnio;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CustomByteBufferPool {

	public static final int LARGE_SIZE = 0x200000;
	private static final ByteBufferPool LARGE_DIRECT = ByteBufferPool.create(LARGE_SIZE, true);
	public static ByteBufferPool POOL = ByteBufferPool.subPool(LARGE_DIRECT, 1024);

	public static ByteBufferPool allocatePool(int bufferSize) {
		return ByteBufferPool.subPool(LARGE_DIRECT, bufferSize);
	}

	public static void test() {
		List<ByteBuffer> buffers = new ArrayList<>();
		try {
			while(true) {
				ByteBuffer buff = POOL.allocate();
				buffers.add(buff);
	
				if(buffers.size() % 1024 == 0) {
					System.out.println("buffer count: " + buffers.size() + " buffer size: "+ buff.capacity());
				}
			}
		}catch(Throwable t) {
			t.printStackTrace();
			System.out.println(buffers.size());
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		//Thread.sleep(20*1000);
		test();
		Thread.sleep(120* 1000);
	}
}