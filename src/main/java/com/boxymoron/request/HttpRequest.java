package com.boxymoron.request;

import java.nio.ByteBuffer;

public interface HttpRequest {
	
	public void parse(ByteBuffer buffer);
	
	public boolean isReadHeader();
	public boolean isReadBody();
	public void setWriteHeader(boolean writeHeader);
	public boolean isWriteHeader();
	public boolean isWriteBody();
	public void reset();
	
}