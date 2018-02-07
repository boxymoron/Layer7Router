package com.boxymoron.request;

import java.nio.ByteBuffer;

public class RequestTester{
	private static final int ITERS = 1000000;
	
	private static String reqStr = "GET /wp-content/uploads/2010/03/hello-kitty-darth-vader-pink.jpg HTTP/1.1\r\n"                                                +
		    "Host: www.kittyhell.com\r\n"                                                                                                  +
		    "User-Agent: Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.6; ja-JP-mac; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3 "             +
		    "Pathtraq/0.9\r\n"                                                                                                             +
		    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n"                                                  +
		    "Accept-Language: ja,en-us;q=0.7,en;q=0.3\r\n"                                                                                 +
		    "Accept-Encoding: gzip,deflate\r\n"                                                                                            +
		    "Accept-Charset: Shift_JIS,utf-8;q=0.7,*;q=0.7\r\n"                                                                            +
		    "Keep-Alive: 115\r\n"                                                                                                          +
		    "Connection: keep-alive\r\n"                                                                                                   +
		    "Cookie: wp_ozh_wsa_visits=2; wp_ozh_wsa_visit_lasttime=xxxxxxxxxx; "                                                          +
		    "__utma=xxxxxxxxx.xxxxxxxxxx.xxxxxxxxxx.xxxxxxxxxx.xxxxxxxxxx.x; "                                                             +
		    "__utmz=xxxxxxxxx.xxxxxxxxxx.x.x.utmccn=(referral)|utmcsr=reader.livedoor.com|utmcct=/reader/|utmcmd=referral\r\n"             +
		    "Expect: 100-continue\r\n" +
		    "Content-Length: 10\r\n" +
		    "\r\n";
	private static String body = "12345=6789";

	public static void main(String[] args) throws InterruptedException {
		
		System.out.println("headers length: "+reqStr.length()+", body length: "+body.length()+", boundary="+(reqStr.indexOf("\r\n\r\n")+4)+", total length: "+(reqStr.length()+body.length()));
		reqStr += body;
		final ByteBuffer buff = ByteBuffer.allocateDirect(reqStr.getBytes().length);
		buff.put(reqStr.getBytes());
		buff.flip();
		
		//Thread.sleep(20000);
		for(int x=0; x< 400; x++) {
			/*
			System.out.println("-------------------------------------------------------------------------------------");
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
			System.out.println("Throughput: "+(((double)ITERS) / (((double)end-(double)start)/1000d))+" per/sec");	
			
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
			
			System.out.println("-------------------------------------------------------------------------------------");
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
			System.out.println(req4);
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
			System.out.println("Throughput: "+(((double)ITERS) / (((double)end5-(double)start5)/1000d))+" per/sec");
			
			System.out.println("-------------------------------------------------------------------------------------");
			System.out.println("Request7.java:");
			buff.rewind();
			Request7 req7 = new Request7(buff);
			buff.rewind();
			System.out.println(req7);
			//Thread.sleep(20000);
			long start7 = System.currentTimeMillis();
			for(int i=0;i<ITERS;i++) {
				//long start2 = System.nanoTime();
				req7 = new Request7(buff);
				buff.rewind();
				//System.out.println("Took: "+(System.nanoTime()-start2));
			}
			long end7 = System.currentTimeMillis();
			System.out.println("Took: "+(end7-start7)+" ms");
			System.out.println("Throughput: "+(((double)ITERS) / (((double)end7-(double)start7)/1000d))+" per/sec");
			*/
			
			System.out.println("-------------------------------------------------------------------------------------");
			System.out.println("Request8.java:");
			buff.rewind();
			Request8 req8 = new Request8(buff);
			buff.rewind();
			System.out.println(req8);
			//Thread.sleep(20000);
			long start8 = System.currentTimeMillis();
			for(int i=0;i<ITERS;i++) {
				//long start2 = System.nanoTime();
				req8 = new Request8(buff);
				buff.rewind();
				//System.out.println("Took: "+(System.nanoTime()-start2));
			}
			long end8 = System.currentTimeMillis();
			System.out.println("Took: "+(end8-start8)+" ms");
			
			System.out.println("Throughput: "+(((double)ITERS) / (((double)end8-(double)start8)/1000d))+" parse per/sec"+" "+( (((double)reqStr.length())*((double)ITERS)) / (((double)end8-(double)start8)/1000d) / (1024d * 1024d) )+" MB/sec" );
		/*
			System.out.println("-------------------------------------------------------------------------------------");
			System.out.println("Request9.java:");
			buff.rewind();
			Request9 req9 = new Request9(buff);
			buff.rewind();
			System.out.println(req9);
			//Thread.sleep(20000);
			long start4 = System.currentTimeMillis();
			for(int i=0;i<ITERS;i++) {
				//long start2 = System.nanoTime();
				req9 = new Request9(buff);
				buff.rewind();
				//System.out.println("Took: "+(System.nanoTime()-start2));
			}
			long end4 = System.currentTimeMillis();
			System.out.println("Took: "+(end4-start4)+" ms");
			System.out.println("Throughput: "+(((double)ITERS) / (((double)end4-(double)start4)/1000d))+" per/sec");	
			*/
		}
	}
}