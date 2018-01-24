import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;

public final class Request {
	final static Logger log = Logger.getLogger(Request.class);
	
	final static boolean isInfo=log.isInfoEnabled();
	final static boolean isDebug=log.isDebugEnabled();
	final static boolean isTrace=log.isTraceEnabled();
	
	private int boundary = -1;
	private boolean isBody = false;
	private String uri;
	private String host;
	private String connection;
	private String expect;
	private int content_length = -1;
	private int body_bytes_read = 0;
	private int body_bytes_written = 0;
	
	final private static char[] HOST = "Host:".toCharArray();
	final private static char[] CONNECTION = "Connection:".toCharArray();
	final private static char[] EXPECT = "Expect:".toCharArray();
	final private static char[] CONTENT_LENGTH = "Content-Length:".toCharArray();
	final private static char[] CONTENT_LENGTH2 = "Content-length:".toCharArray();
	
	public Request(ByteBuffer buffer) {
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
		int pos = buffer.position();
		final CharBuffer cbuff = StandardCharsets.ISO_8859_1.decode(buffer);
		buffer.position(pos);
		if(cbuff.remaining() < 4) {
			this.isBody = true;
			return;
		}
		int boundary = -1;
		for(int i=0;i+3<cbuff.remaining();i++) {
			if(cbuff.charAt(i) == '\r' && cbuff.charAt(i+1) == '\n' && cbuff.charAt(i+2) == '\r' && cbuff.charAt(i+3) == '\n') {
				boundary = i+4;
				break;
			}
		}
		this.boundary = boundary;
		if(boundary == -1) {//If no \r\n\r\n was found we're probably in the body, reset this.boundary to -1 and skip parsing logic below.
			this.isBody = true;
			return;
		}
		
		int body_length_in_this_buffer = buffer.limit() - boundary;
		body_bytes_read += body_length_in_this_buffer;
		int prev=0;
		final char[] arr = cbuff.array();
		for(int i=0;i<arr.length && prev+2<arr.length;i++) {
			if(cbuff.charAt(i) == '\r') {
				if(prev == 0) {
					uri = new String(arr, prev, i);
				} else {
					if(fastStartsWith(arr, prev, HOST)) {
						host = new String(arr, prev+HOST.length+1, i-(prev+HOST.length+1));
					}else if(fastStartsWith(arr, prev, CONNECTION)) {
						connection = new String(arr, prev+CONNECTION.length+1, i-(prev+CONNECTION.length+1));
					}else if(fastStartsWith(arr, prev, CONNECTION)) {
						connection = new String(arr, prev+CONNECTION.length+1, i-(prev+CONNECTION.length+1));
					}else if(fastStartsWith(arr, prev, EXPECT)) {
						expect = new String(arr, prev+EXPECT.length+1, i-(prev+EXPECT.length+1));
					}else if(fastStartsWith(arr, prev, CONTENT_LENGTH) || fastStartsWith(arr, prev, CONTENT_LENGTH2)) {
						content_length = Integer.parseInt(new String(arr, prev+CONTENT_LENGTH.length+1, i-(prev+CONTENT_LENGTH.length+1)));
					}
				}
				prev=i+2;
			}
			
		}
		if(uri == null && host == null) {
			return;
		}
		if(boundary > 0) {
			buffer.position(boundary);
		}
	}
	
	private final static boolean fastStartsWith(char[] arr, int off, char[] str) {
		for(int i=0;i<str.length;i++) {
			if(arr[off+i] != str[i]) {
				return false;
			}
		}
		return true;
	}
	
	public static void main(String[] args) throws InterruptedException {
		String reqStr = "POST /path/index.html HTTP/1.1\r\n" + 
		"Host: localhost\r\n" + 
		"Connection: keep-alive\r\n" + 
		"Expect: 100-continue\r\n" + 
		"User-Agent: Chrome/1.0\r\n" + 
		"Content-Type: application/x-www-form-urlencoded\r\n" + 
		"Content-length: 32\r\n" + 
		"\r\n";
		
		String body = "foo=bar&asdf+asdf=asdf";
		//System.out.println("req, headers length: "+reqStr.length()+" body length: "+body.length()+" contents:\n"+reqStr+body);
		reqStr += body;
		//System.out.println(reqStr.indexOf("\r\n\r\n"));
		final ByteBuffer buff = ByteBuffer.allocate(reqStr.getBytes().length);
		buff.put(reqStr.getBytes());
		buff.flip();
		
		Request req = new Request(buff);
		buff.rewind();
		System.out.println(req);
		//Thread.sleep(30000);
		long start = System.currentTimeMillis();
		for(int i=0;i<20000000;i++) {
			Request req2 = new Request(buff);
			buff.rewind();
		}
		long end = System.currentTimeMillis();
		System.out.println("Took: "+(end-start)+" ms");
		System.out.println("Throughput: "+(20000000d / (((double)end-(double)start)/1000d))+" per/sec");
		//System.out.println(req);
		
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
		builder.append("Request [boundary=").append(boundary).append(", isBody=").append(isBody).append(", uri=")
				.append(uri).append(", host=").append(host).append(", connection=").append(connection)
				.append(", expect=").append(expect).append(", content_length=").append(content_length)
				.append(", body_bytes_read=").append(body_bytes_read).append(", body_bytes_written=")
				.append(body_bytes_written).append("]");
		return builder.toString();
	}

}