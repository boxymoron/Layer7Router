package com.boxymoron.request;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;

public final class Request3 {
	private static final int EXPECT_LEN = "\nExpect: ".length();

	private static final int CONNECTION_LEN = "\nConnection: ".length();

	private static final int CONTENT_LENGTH_LEN = "\nContent-Length: ".length();

	private static final int HOST_LEN = "\nHost: ".length();

	final static Logger log = Logger.getLogger(Request3.class);
	
	private int boundary = -1;
	private volatile boolean isBody = false;
	private String uri;
	private String host;
	private String connection;
	private String expect;
	private int content_length = -1;
	private int body_bytes_read = 0;
	private int body_bytes_written = 0;
	
	public Request3(ByteBuffer buffer) {
		parseRequest(buffer);
	}
	
	public final void parseRequest(ByteBuffer buffer) {		
		if(isBody || boundary != -1) {
			final int read_so_far = body_bytes_read + buffer.remaining();
			if(read_so_far < content_length) {
				if(log.isDebugEnabled())log.debug("in body:");
				body_bytes_read = read_so_far;
				return;
			} else if(read_so_far == content_length) {
				
				body_bytes_read = read_so_far;
				if(log.isDebugEnabled())log.debug("finished reading body\n"+this.toString());
				if(body_bytes_written == content_length) {
					if(log.isDebugEnabled())log.debug("finished writing body");
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
		
		log.debug("Parsing...");
		final String content = StandardCharsets.US_ASCII.decode(buffer).toString();
		int boundary = content.indexOf("\r\n\r\n");
		if(boundary > 0) {
			boundary += 4;
			content.substring(0, boundary);
		}
		if(this.boundary == -1) {
			this.boundary = boundary;
		}else {
			isBody = true;
		}
		
		//if(log.isDebugEnabled())log.debug("Headers:\n---------------------\n"+content.substring(0, boundary < 0 ? content.length() : boundary)+"\n------------------------------------------------------");
		//if(log.isDebugEnabled())log.debug("Body:\n------------------------\n"+content.substring(boundary < 0 ? content.length() : boundary)+"\n------------------------------------------------------");
		
		int body_length_in_this_buffer = buffer.limit() - boundary;
		body_bytes_read += body_length_in_this_buffer;
		final String[] headersArray = content.split("\r");
		for(String header : headersArray) {
			if(host != null && connection != null && expect != null && content_length != -1) {
				break;
			}
			if(uri == null) {
				uri = header;
			}else if(host == null && header.indexOf("\nHost: ") == 0) {
				host = header.substring(HOST_LEN, header.length());
			}else if(connection == null && header.indexOf("\nConnection: ") == 0) {
				connection = header.substring(CONNECTION_LEN, header.length());
			}else if(expect == null && header.indexOf("\nExpect: ") == 0) {
				expect = header.substring(EXPECT_LEN, header.length());
			}else if(content_length == -1 && header.indexOf("\nContent-Length: ") == 0 || header.indexOf("\nContent-length: ") == 0) {
				content_length = Integer.parseInt(header.substring(CONTENT_LENGTH_LEN, header.length()));
			}
		}
		if(uri == null && host == null) {
			return;
		}
		if(boundary > 0) {
			buffer.position(boundary);
		}
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
		builder.append("Request3 [boundary=").append(boundary).append(", isBody=").append(isBody).append(", uri=")
				.append(uri).append(", host=").append(host).append(", connection=").append(connection)
				.append(", expect=").append(expect).append(", content_length=").append(content_length)
				.append(", body_bytes_read=").append(body_bytes_read).append(", body_bytes_written=")
				.append(body_bytes_written).append("]");
		return builder.toString();
	}

}