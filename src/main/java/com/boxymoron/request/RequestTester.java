package com.boxymoron.request;

import java.nio.ByteBuffer;

public class RequestTester{
	private static final int ITERS = 1000000;

	public static void main(String[] args) throws InterruptedException {
		String reqStr = "POST /path/index.html HTTP/1.1\r\n" + 
						"Host: localhost\r\n" + 
						"Connection: keep-alive\r\n" + 
						"Expect: 100-continue\r\n" + 
						"User-Agent: Chrome/1.11\r\n" +  
						"Cookie: zzzzzzzzzz\r\n" +  
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"asdf: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n" +
						"Content-length: 10\r\n" + 
						"\r\n";
				
		String body = "12345=6789";
		System.out.println("headers length: "+reqStr.length()+", body length: "+body.length()+", boundary="+(reqStr.indexOf("\r\n\r\n")+4)+", total length: "+(reqStr.length()+body.length()));
		reqStr += body;
		final ByteBuffer buff = ByteBuffer.allocateDirect(reqStr.getBytes().length);
		buff.put(reqStr.getBytes());
		buff.flip();
		//Thread.sleep(20000);
		for(int x=0; x< 4; x++) {
			/*System.out.println("-------------------------------------------------------------------------------------");
			System.out.println("Request.java:");
			Request req = new Request(buff);
			buff.rewind();
			System.out.println(req);
			//Thread.sleep(20000);
			long start = System.currentTimeMillis();
			for(int i=0;i<ITERS;i++) {
				//long start2 = System.nanoTime();
				req = new Request(buff);
				buff.rewind();
				//System.out.println("Took: "+(System.nanoTime()-start2));
			}
			long end = System.currentTimeMillis();
			System.out.println("Took: "+(end-start)+" ms");
			System.out.println("Throughput: "+(((double)ITERS) / (((double)end-(double)start)/1000d))+" per/sec");	*/
			
			System.out.println("-------------------------------------------------------------------------------------");
			System.out.println("Request2.java:");
			buff.rewind();
			Request2 req2 = new Request2(buff);
			buff.rewind();
			System.out.println(req2);
			//Thread.sleep(20000);
			long start2 = System.currentTimeMillis();
			for(int i=0;i<ITERS;i++) {
				//start2 = System.nanoTime();
				req2 = new Request2(buff);
				buff.rewind();
				//System.out.println("Took: "+(System.nanoTime()-start2));
			}
			long end2 = System.currentTimeMillis();
			System.out.println("Took: "+(end2-start2)+" ms");
			System.out.println("Throughput: "+(((double)ITERS) / (((double)end2-(double)start2)/1000d))+" per/sec");		
			
			/*System.out.println("-------------------------------------------------------------------------------------");
			System.out.println("Request3.java:");
			buff.rewind();
			Request3 req3 = new Request3(buff);
			buff.rewind();
			System.out.println(req3);
			//Thread.sleep(20000);
			long start3 = System.currentTimeMillis();
			for(int i=0;i<ITERS;i++) {
				//long start2 = System.nanoTime();
				req3 = new Request3(buff);
				buff.rewind();
				//System.out.println("Took: "+(System.nanoTime()-start2));
			}
			long end3 = System.currentTimeMillis();
			System.out.println("Took: "+(end3-start3)+" ms");
			System.out.println("Throughput: "+(((double)ITERS) / (((double)end3-(double)start3)/1000d))+" per/sec");	
			
			System.out.println("-------------------------------------------------------------------------------------");
			System.out.println("Request4.java:");
			buff.rewind();
			Request4 req4 = new Request4(buff);
			buff.rewind();
			System.out.println(req3);
			//Thread.sleep(20000);
			long start4 = System.currentTimeMillis();
			for(int i=0;i<ITERS;i++) {
				//long start2 = System.nanoTime();
				req4 = new Request4(buff);
				buff.rewind();
				//System.out.println("Took: "+(System.nanoTime()-start2));
			}
			long end4 = System.currentTimeMillis();
			System.out.println("Took: "+(end4-start4)+" ms");
			System.out.println("Throughput: "+(((double)ITERS) / (((double)end4-(double)start4)/1000d))+" per/sec");	
			
			System.out.println("-------------------------------------------------------------------------------------");
			System.out.println("Request5.java:");
			buff.rewind();
			Request5 req5 = new Request5(buff);
			buff.rewind();
			System.out.println(req5);
			//Thread.sleep(20000);
			long start5 = System.currentTimeMillis();
			for(int i=0;i<ITERS;i++) {
				//long start2 = System.nanoTime();
				req5 = new Request5(buff);
				buff.rewind();
				//System.out.println("Took: "+(System.nanoTime()-start2));
			}
			long end5 = System.currentTimeMillis();
			System.out.println("Took: "+(end5-start5)+" ms");
			System.out.println("Throughput: "+(((double)ITERS) / (((double)end5-(double)start5)/1000d))+" per/sec");	*/
		}
	}
}