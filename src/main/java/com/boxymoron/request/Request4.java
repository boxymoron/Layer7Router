package com.boxymoron.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.log4j.Logger;

public final class Request4 {
	final static Logger log = Logger.getLogger(Request.class);
	
	final private static boolean isInfo=log.isInfoEnabled();
	final private static boolean isDebug=log.isDebugEnabled();
	final private static boolean isTrace=log.isTraceEnabled();
	
	final private static char[] HOST = "Host:".toCharArray();
	final private static int HOST_LEN = HOST.length + 1;
	
	final private static char[] CONNECTION = "Connection:".toCharArray();
	final private static int CONNECTION_LEN = CONNECTION.length + 1;
	
	final private static char[] KEEP = "keep".toCharArray();
	
	final private static char[] EXPECT = "Expect:".toCharArray();
	final private static int EXPECT_LEN = EXPECT.length + 1;
	
	final private static char[] CONTENT_LENGTH = "Content-Length:".toCharArray();
	final private static char[] CONTENT_LENGTH2 = "Content-length:".toCharArray();
	final private static int CONTENT_LENGTH_LEN = CONTENT_LENGTH.length + 1;
	
	private int boundary = -1;
	private boolean isBody = false;
	private String uri;
	private String host;
	private String connection;
	private boolean keepalive;
	private String expect;
	private int content_length = -1;
	private int body_bytes_read = 0;
	private int body_bytes_written = 0;

	public Request4(ByteBuffer buffer) {
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

		//final CharBuffer cbuff = StandardCharsets.ISO_8859_1.decode(buffer);
		final int remaining = buffer.remaining();
		if(remaining < 4) {
			this.isBody = true;
			return;
		}
		
		final char[] chars = new char[remaining];
		for (int i = 0; i < remaining; i++) {
		    chars[i] = (char) (buffer.get(i + buffer.position()) & 0xFF);
		}
		
		final CharBuffer cbuff = CharBuffer.wrap(chars);
		int prev=0;
		//long start2 = System.nanoTime();
		for(int i=0;i<remaining && prev+2 < remaining;i++) {
			if(host != null && connection != null && expect != null && content_length != -1) {
				break;
			}
			
			if(chars[i] == '\r') {
				if(prev == 0) {
					uri = new String(chars, prev, i);
				} else {
					if(host == null && Arrays.equals(Arrays.copyOfRange(chars, prev, prev+HOST_LEN-1), HOST)) {
						final int start = prev+HOST_LEN;
						host = new String(chars, start, i-start);
					}else if(connection == null && Arrays.equals(Arrays.copyOfRange(chars, prev, prev+CONNECTION_LEN-1), CONNECTION)) {
						final int start = prev+CONNECTION_LEN;
						connection = new String(chars, start, i-start);
						if(connection.equals("keep-alive")) {
							keepalive = true;
						}
					}else if(expect == null && Arrays.equals(Arrays.copyOfRange(chars, prev, prev+EXPECT_LEN-1), EXPECT)) {
						final int start = prev+EXPECT_LEN;
						expect = new String(chars, start, i-start);
					}else if(content_length == -1 && (Arrays.equals(Arrays.copyOfRange(chars, prev, prev+CONTENT_LENGTH_LEN-1), CONTENT_LENGTH) || Arrays.equals(Arrays.copyOfRange(chars, prev, prev+CONTENT_LENGTH_LEN-1), CONTENT_LENGTH2))) {
						final int start = prev+CONTENT_LENGTH_LEN;
						//content_length = parseInt(chars, start, i);
						content_length = Integer.parseInt(new String(chars, start, i-start));
						boundary = (remaining - content_length)+1;
						if(host != null && connection != null && expect != null && content_length != -1) {
							break;
						}
					}
				}
				if(i+3 < remaining && cbuff.charAt(i+1) == '\n' && cbuff.charAt(i+2) == '\r' && cbuff.charAt(i+3) == '\n') {
					boundary = i+4;
					break;
				}
				prev=(i+2);
			}
		}
		
		if(content_length != -1) {
			boundary = remaining - content_length;	
			body_bytes_read += buffer.limit() - boundary;
		}
		if(boundary == -1) {//If no \r\n\r\n was found we're probably in the body, reset this.boundary to -1 and skip parsing logic below.
			this.isBody = true;
			return;
		}
		//System.out.println("Took: "+(System.nanoTime()-start2));
		
		if(uri == null || host == null) {
			return;
		}
		if(boundary > 0) {
			buffer.position(boundary);
		}
	}

    private final static boolean startsWith(char[] string, char[] prefix, int off) {
        int po = 0;
        int pc = prefix.length;
        if ((off < 0) || (off > string.length - pc)) {
            return false;
        }
        while (--pc >= 0) {
            if (string[off++] != prefix[po++]) {
                return false;
            }
        }
        return true;
    }
	
	private final static int parseInt(char[] arr,int start, int end){
	    int intValue = 0;
	    int decimal = 1;
	    for(int index = end ; index > start ; index --){
	        intValue = intValue + (((int)arr[index-1] - 48) * (decimal));
	        decimal = decimal * 10;
	    }
	    return intValue;
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
		return keepalive;
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
		builder.append("Request4 [boundary=").append(boundary).append(", isBody=").append(isBody).append(", uri=")
				.append(uri).append(", host=").append(host).append(", connection=").append(connection)
				.append(", keepalive=").append(keepalive).append(", expect=").append(expect).append(", content_length=")
				.append(content_length).append(", body_bytes_read=").append(body_bytes_read)
				.append(", body_bytes_written=").append(body_bytes_written).append("]");
		return builder.toString();
	}

}