package com.boxymoron.request;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;

public final class Request2 {
	final static Logger log = Logger.getLogger(Request2.class);
	
	final static boolean isInfo=log.isInfoEnabled();
	final static boolean isDebug=log.isDebugEnabled();
	final static boolean isTrace=log.isTraceEnabled();
	final private static byte[] HOST = "Host:".getBytes();
	final private static byte[] CONNECTION = "Connection:".getBytes();
	final private static byte[] EXPECT = "Expect:".getBytes();
	final private static byte[] CONTENT_LENGTH = "Content-Length:".getBytes();
	final private static byte[] CONTENT_LENGTH2 = "Content-length:".getBytes();
	
	final private static int DIVIDER = ByteBuffer.wrap("\r\n\r\n".getBytes()).getInt();
	
	private int boundary = -1;
	private boolean isBody = false;
	private String uri;
	private String host;
	private String connection;
	private String expect;
	private int content_length = -1;
	private int body_bytes_read = 0;
	private int body_bytes_written = 0;

	public Request2(ByteBuffer buffer) {
		parseRequest(buffer);
	}
	
	public final void parseRequest(ByteBuffer buffer) {
		if(isBody || boundary != -1) {
			final int read_so_far = body_bytes_read + buffer.remaining();
			if(read_so_far < content_length) {
				if(isDebug)log.debug("in body:");
				body_bytes_read = read_so_far;
				return;
			} else if(read_so_far == content_length) {
				body_bytes_read = read_so_far;
				if(isDebug)log.debug("finished reading body\n"+this.toString());
				if(body_bytes_written == content_length) {
					if(isDebug)log.debug("finished writing body");
					boundary = -1;
					isBody = false;
				}
				return;
			} else if (read_so_far > content_length){
				int delta = content_length - body_bytes_read;
				if(delta != 0 && log.isDebugEnabled()) {
					log.warn("delta: "+delta);
					int pos = buffer.position();
					String content = StandardCharsets.US_ASCII.decode(buffer).toString();
					log.warn("Full Content:");
					log.warn(content);
					buffer.position(pos);
					log.warn("body fragment: ");
					log.warn(content.substring(0, delta));
					
					log.warn("New request on same connection:");
					log.warn(content.substring(delta));
					
					log.warn(this);
				}
				body_bytes_read = 0;
				boundary = -1;
				isBody = false;
			}
		}
		
		if(isDebug)log.debug("Parsing...");

		int remaining = buffer.remaining();
		body_bytes_read += (buffer.limit()-1) - boundary;
		int prev=0;
		//long start2 = System.nanoTime();
		final int limit = buffer.limit();
		for(short i=0;i<limit && prev+2<limit;i++) {
			if(host != null && connection != null && expect != null && content_length != -1) {
				break;
			}
			if(buffer.get(i) == '\r') {
				if(content_length != -1) {
					boundary = remaining - content_length;
				}
				if(boundary != -1 && i > boundary) {
					break;
				}
				if(host != null && connection != null && expect != null && content_length != -1) {
					break;
				}
				if(prev == 0) {
					uri = subString(buffer, prev, i);
				} else if(host == null && eq(buffer, HOST, prev, (prev)+HOST.length)) {
					host = subString(buffer, prev+HOST.length+1, i);
				}else if(connection == null && eq(buffer, CONNECTION, prev, (prev)+CONNECTION.length)) {
					connection = subString(buffer, prev+CONNECTION.length+1, i);
				}else if(expect == null && eq(buffer, EXPECT, prev, (prev+EXPECT.length))) {
					expect = subString(buffer, prev+EXPECT.length+1, i);
				}else if(content_length == -1 && eq(buffer, CONTENT_LENGTH, prev, (prev)+CONTENT_LENGTH.length) || eq(buffer, CONTENT_LENGTH2, prev, (prev)+CONTENT_LENGTH2.length)) {
					String c2 = subString(buffer, prev+CONTENT_LENGTH.length+1, i);
					content_length = Integer.parseInt(c2);
				}

				if(i+3 < remaining && buffer.get(i+1) == '\n' && buffer.get(i+2) == '\r' && buffer.get(i+3) == '\n') {
					boundary = i+4;
					break;
				}
				prev = (i+2);
			}
		}
		//System.out.println("Took: "+(System.nanoTime()-start2));
		
		if(uri == null || host == null) {
			return;
		}
		if(boundary > 0) {
			buffer.position(boundary);
		}
	}
	
	private static String subString(ByteBuffer buffer, int start, int end) {
		byte[] arr = new byte[end - start];
		for(int i=0;i<arr.length;i++) {
			arr[i] = buffer.get(start+i);
		}
		return new String(arr);
	}

	private static boolean eq(ByteBuffer buffer, final byte[] arrA, int i, int j) {
		for(int y=0,x=i;x<j;y++,x++) {
			if(arrA[y] != buffer.get(x)) {
				return false;
			}
		}
		return true;
	}
	
	public static long batol(byte[] buff) {
	    return batol(buff, false);
	}

	public static long batol(byte[] buff, boolean littleEndian) {
	    assert(buff.length == 8);
	    ByteBuffer bb = ByteBuffer.wrap(buff);
	    if (littleEndian) bb.order(ByteOrder.LITTLE_ENDIAN);
	    return bb.getLong();
	}
	
	public boolean isMoreToRead() {
		return this.body_bytes_read < content_length;
	}
	
	public boolean isMoreToWrite() {
		return this.body_bytes_written < content_length;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getConnection() {
		return connection;
	}
	
	public boolean isKeepAlive() {
		return "keep-alive".equalsIgnoreCase(connection);
	}

	public void setConnection(String connection) {
		this.connection = connection;
	}

	public int getContent_length() {
		return content_length;
	}

	public void setContent_length(int content_length) {
		this.content_length = content_length;
	}

	public int getBoundary() {
		return boundary;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public int getBody_bytes_read() {
		return body_bytes_read;
	}
	
	public void inc_reads(int count) {
		this.body_bytes_read += count;
	}

	public void setBody_bytes_read(int body_bytes_read) {
		this.body_bytes_read = body_bytes_read;
	}

	public String getExpect() {
		return expect;
	}

	public void setExpect(String expect) {
		this.expect = expect;
	}

	public boolean isBody() {
		return isBody;
	}

	public void setBody(boolean isBody) {
		this.isBody = isBody;
	}
	
	public void inc_writes(int count) {
		this.body_bytes_written += count;
	}

	public int getBody_bytes_written() {
		return body_bytes_written;
	}

	public void setBody_bytes_written(int body_bytes_written) {
		this.body_bytes_written = body_bytes_written;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Request2 [boundary=").append(boundary).append(", isBody=").append(isBody).append(", uri=")
				.append(uri).append(", host=").append(host).append(", connection=").append(connection)
				.append(", expect=").append(expect).append(", content_length=").append(content_length)
				.append(", body_bytes_read=").append(body_bytes_read).append(", body_bytes_written=")
				.append(body_bytes_written).append("]");
		return builder.toString();
	}

}