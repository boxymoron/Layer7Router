package com.boxymoron.request;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.log4j.Logger;

public final class Request8 implements HttpRequest {	
	final static Logger log = Logger.getLogger(Request3.class);
	
	final static boolean isInfo=log.isInfoEnabled();
	final static boolean isDebug=log.isDebugEnabled();
	final static boolean isTrace=log.isTraceEnabled();
	
	private int boundary = -1;

	private String uri;
	private String host;
	private String connection;
	private String expect;

	private int contentLength = -1;
	private int bodyBytesRead = 0;
	private int bodyBytesWritten = 0;
	
	private boolean readHeader=true;
	private boolean readBody;
	private boolean writeHeader;
	private boolean writeBody;

	public Request8(ByteBuffer buffer) {
		parseRequest(buffer);
	}
	
	public final void parseRequest(ByteBuffer buffer) {
		if(boundary != -1) {
			bodyBytesRead += buffer.remaining();
			if(bodyBytesRead < contentLength) {
				String content = StandardCharsets.US_ASCII.decode(buffer).toString();
				if(isDebug)log.debug("in body: \n"+content);
				return;
			} else if(bodyBytesRead == contentLength) {
				if(isDebug)log.debug("finished reading body\n"+this.toString());
				String content = StandardCharsets.US_ASCII.decode(buffer).toString();
				if(isDebug)log.debug("in body: \n"+content);
				writeBody=true;
				if(bodyBytesWritten == contentLength) {
					if(isDebug)log.debug("finished writing body");
					contentLength = -1;
					bodyBytesRead=0;
					bodyBytesWritten=0;
					boundary = -1;
				}
				return;
			} else if (bodyBytesRead > contentLength){
				int delta = contentLength - bodyBytesRead;
				if(delta != 0 && log.isDebugEnabled()) {
					log.warn("Delta: "+delta);
					int pos = buffer.position();
					String content = StandardCharsets.US_ASCII.decode(buffer).toString();
					log.warn("Full Content:");
					log.warn("\n"+content);
					buffer.position(pos);
					log.warn(this);
				}
				contentLength = -1;
				bodyBytesRead = 0;
				boundary = -1;
			}
		}
		
		if(isDebug)log.debug("Parsing...");

		final int remaining = buffer.remaining();
		
		final String content = getContent(buffer, remaining);
		if(isDebug) {
			log.debug("content:\n"+content);
		}
		final int length = content.length();
		int biggestHeaderIdx = -1;
		int uriIdx = setURI(content);
		if(uriIdx > biggestHeaderIdx) {
			biggestHeaderIdx = uriIdx;
		}
		final int hostIdx = setHost(content, length);
		if(hostIdx > biggestHeaderIdx) {
			biggestHeaderIdx = hostIdx;
		}
		final int connectionIdx = setConnection(content, length);
		if(connectionIdx > biggestHeaderIdx) {
			biggestHeaderIdx = connectionIdx;
		}
		final int expectsIdx = setExpects(content, length);
		if(expectsIdx > biggestHeaderIdx) {
			biggestHeaderIdx = expectsIdx;
		}
		final int contentLengthIdx = setContentLength1(content, length);
		if(contentLengthIdx > biggestHeaderIdx) {
			biggestHeaderIdx = contentLengthIdx;
		}
		if(contentLength == -1) {
			final int contentLengthIdx2 = setContentLength2(content, length);
			if(contentLengthIdx2 > biggestHeaderIdx) {
				biggestHeaderIdx = contentLengthIdx2;
			}
		}
		
		final int boundary = biggestHeaderIdx > -1 ? content.indexOf("\r\n\r\n", biggestHeaderIdx)+4 : content.indexOf("\r\n\r\n")+4;
		//int boundary = content.indexOf("\r\n\r\n")+4;
		this.boundary = boundary;
		if(contentLength > -1) {
			if(this.boundary + contentLength > remaining) {//
				readHeader = false;
				readBody = true;
			}else if(this.boundary + contentLength == remaining) {
				readHeader = false;
				readBody = false;
				writeHeader = true;
				writeBody = true;
				buffer.position(this.boundary + contentLength);
				this.bodyBytesRead = contentLength;
				this.boundary = -1;
				return;
			} else {//this.boundary + content_length < remaining
				this.boundary = remaining - contentLength;
				if(this.boundary < 0) {
					this.boundary = boundary;
				}
			}
		}
		if(boundary > -1) {
			readHeader = false;
			writeHeader = true;
			int body_length_in_this_buffer = remaining - (this.boundary);
			if(body_length_in_this_buffer > -1) {
				bodyBytesRead += body_length_in_this_buffer;
			}
		}
		if(uri == null && host == null) {
			return;
		}
		if(boundary > 0) {
			if(boundary <= buffer.limit()) {
				buffer.position(boundary);
			}else {
				log.error("boundary > buffer.limit. boundary: "+boundary+" buffer.limit: "+buffer.limit());
				if(isDebug) {
					log.debug("content:\n"+content);
				}
			}
		}
	}

	private final int setURI(final String content) {
		int uriIdx = content.indexOf("\r\n");
		if(uriIdx > -1) {
			uri = content.substring(0, uriIdx);
		}
		return uriIdx;
	}

	private final int setContentLength2(final String content, final int length) {
		final int content_length2Idx = content.indexOf("\r\nContent-length: ");
		if(content_length2Idx > -1) {
			for(int i=content_length2Idx+18;i<length;i++) {
				if(content.charAt(i) == '\r') {
					contentLength = Integer.parseInt(content.substring(content_length2Idx+18, i));
					break;
				}
			}
		}
		return content_length2Idx;
	}

	private final int setContentLength1(final String content, final int length) {
		final int content_lengthIdx = content.indexOf("\r\nContent-Length: ");
		if(content_lengthIdx > -1) {
			for(int i=content_lengthIdx+18;i<length;i++) {
				if(content.charAt(i) == '\r') {
					contentLength = Integer.parseInt(content.substring(content_lengthIdx+18, i));
					break;
				}
			}
		}
		return content_lengthIdx;
	}

	private final int setExpects(final String content, final int length) {
		final int expectIdx = content.indexOf("\r\nExpect: ");
		if(expectIdx > -1) {
			for(int i=expectIdx+10+2;i<length;i++) {
				if(content.charAt(i) == '\r') {
					expect = content.substring(expectIdx+10, i);
					break;
				}
			}
		}
		return expectIdx;
	}

	private final int setConnection(final String content, final int length) {
		final int connectionIdx = content.indexOf("\r\nConnection: ");
		if(connectionIdx > -1) {
			for(int i=connectionIdx+14+2;i<length;i++) {
				if(content.charAt(i) == '\r') {
					connection = content.substring(connectionIdx+14, i);
					break;
				}
			}
		}
		return connectionIdx;
	}

	private final int setHost(final String content, final int length) {
		final int hostIdx = content.indexOf("\r\nHost: ");
		if(hostIdx > -1) {
			for(int i=hostIdx+8+2;i<length;i++) {
				if(content.charAt(i) == '\r') {
					host = content.substring(hostIdx+8, i);
					break;
				}
			}
		}
		return hostIdx;
	}

	private final String getContent(final ByteBuffer buffer, final int remaining) {
		final char[] chars = new char[remaining];
		for (int i = 0; i < remaining; i++) {
		    chars[i] = (char) (buffer.get(i + buffer.position()) & 0xFF);
		}
		return new String(chars);
	}
	
	public boolean isMoreToRead() {
		return bodyBytesRead < contentLength;
	}
	
	public boolean isMoreToWrite() {
		return bodyBytesWritten < contentLength;
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

	public int getContentLength() {
		return contentLength;
	}

	public void setContentLength(int content_length) {
		this.contentLength = content_length;
	}

	public int getBoundary() {
		return boundary;
	}

	public void setBoundary(int boundary) {
		this.boundary = boundary;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public int getBodyBytesRead() {
		return bodyBytesRead;
	}
	
	public void incrementBodyBytesRead(int count) {
		this.bodyBytesRead += count;
	}

	public void setBodyBytesRead(int body_bytes_read) {
		this.bodyBytesRead = body_bytes_read;
	}

	public String getExpect() {
		return expect;
	}

	public void setExpect(String expect) {
		this.expect = expect;
	}
	
	public void incrementBodyBytesWritten(int count) {
		this.bodyBytesWritten += count;
	}

	public int getBodyBytesWritten() {
		return bodyBytesWritten;
	}

	public void setBodyBytesWritten(int body_bytes_written) {
		this.bodyBytesWritten = body_bytes_written;
	}
	
	public int remainingWrites() {
		if(expect != null) {
			try {
				int expect = Integer.parseInt(this.expect);
				return expect - bodyBytesWritten;
			}catch(NumberFormatException e) {
				
			}
		}
		return contentLength - bodyBytesWritten;
	}
	
	@Override
	public void parse(ByteBuffer buffer) {
		parseRequest(buffer);
	}

	@Override
	public boolean isReadHeader() {
		return readHeader;
	}

	@Override
	public boolean isReadBody() {
		return readBody;
	}

	@Override
	public boolean isWriteHeader() {
		return writeHeader;
	}

	@Override
	public boolean isWriteBody() {
		return writeBody;
	}
	
	@Override
	public void setWriteHeader(boolean writeHeader) {
		this.writeHeader = writeHeader;
	}

	@Override
	public void reset() {
		this.bodyBytesRead=0;
		this.bodyBytesWritten=0;
		this.connection=null;
		this.boundary=-1;
		this.contentLength=-1;
		this.expect=null;
		this.host=null;
		this.readBody=false;
		this.readHeader=true;
		this.uri=null;
		this.writeBody=false;
		this.writeHeader=false;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Request8 [boundary=").append(boundary).append(", uri=")
				.append(uri).append(", host=").append(host).append(", connection=").append(connection)
				.append(", expect=").append(expect).append(", content_length=").append(contentLength)
				.append(", body_bytes_read=").append(bodyBytesRead).append(", body_bytes_written=")
				.append(bodyBytesWritten).append("]");
		return builder.toString();
	}

}