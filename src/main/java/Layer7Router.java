import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.xnio.ByteBufferPool;
import org.xnio.ByteBufferSlicePool;
import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.IoFuture.Status;
import org.xnio.OptionMap;

import org.xnio.Pooled;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;

/**
 * TODO: Look into cpu/thread affinity.
 * @author royer
 *
 */
@SuppressWarnings("deprecation")
public final class Layer7Router {

	final static Logger log = Logger.getLogger(Layer7Router.class);

	final static Xnio xnio = Xnio.getInstance();
	final static OptionMap xnioOptions = OptionMap.builder()
			.set(org.xnio.Options.ALLOW_BLOCKING, false)
			.set(org.xnio.Options.RECEIVE_BUFFER, 1024*8)
			.set(org.xnio.Options.SEND_BUFFER, 1024*8)
			//.set(org.xnio.Options.READ_TIMEOUT, 30000)
			//.set(org.xnio.Options.WRITE_TIMEOUT, 30000)
			.set(org.xnio.Options.USE_DIRECT_BUFFERS, true)
			.set(org.xnio.Options.WORKER_IO_THREADS, 2)
			.set(org.xnio.Options.SPLIT_READ_WRITE_THREADS, false)
			.set(org.xnio.Options.BACKLOG, 8192)
			.set(org.xnio.Options.KEEP_ALIVE, false)
			.getMap();
	static XnioWorker worker;
	
	final static AtomicInteger totalAccepted = new AtomicInteger();
	final static AtomicInteger sessionsCount = new AtomicInteger();
	final static AtomicLong globalBackendWriteBytes = new AtomicLong();
	final static AtomicLong globalClientWriteBytes = new AtomicLong();
	final static AtomicLong globalBackendReadBytes = new AtomicLong();
	final static AtomicLong globalClientReadBytes = new AtomicLong();

	//static ByteBufferSlicePool pool = new ByteBufferSlicePool(1024*8, 32*1024*1024*32);
	static ByteBufferPool pool = ByteBufferPool.MEDIUM_DIRECT;
	
	public static Options routerOptions = new Options();
	
	final static boolean isInfo=log.isInfoEnabled();
	final static boolean isDebug=log.isDebugEnabled();
	final static boolean isTrace=log.isTraceEnabled();

	public static void main(String[] args) throws Exception {
		final CmdLineParser cmdLineParser = new CmdLineParser(routerOptions);
		cmdLineParser.parseArgument(args);
		System.out.println(routerOptions.toString());

		worker = xnio.createWorker(xnioOptions);
		
		
		final Deque<ReadListener> readListeners = new ConcurrentLinkedDeque<>();

		final ChannelListener<AcceptingChannel<StreamConnection>> acceptListener2 = new ChannelListener<AcceptingChannel<StreamConnection>>() {
			@Override
			public final void handleEvent(AcceptingChannel<StreamConnection> channel) {
				try {
					StreamConnection accepted;
					while ((accepted = channel.accept()) != null) {
						if(isDebug)log.debug("Accepted: " + accepted.getPeerAddress());
						totalAccepted.incrementAndGet();
						final ReadListener readListener = new ReadListener();
						accepted.getSourceChannel().setReadListener(readListener);
						accepted.getSourceChannel().setCloseListener(readListener);
						readListener.streamConnection = accepted;
						sessionsCount.incrementAndGet();
						
						final WriteListener writeListener = new WriteListener(readListener);
						accepted.getSinkChannel().setWriteListener(writeListener);
						accepted.getSinkChannel().setCloseListener(writeListener);
						readListener.writeListener = writeListener;

						accepted.getSourceChannel().resumeReads();
						accepted.getSinkChannel().resumeWrites();
						readListeners.push(readListener);
					}
				} catch (IOException e) {
					log.error("", e);
					try {
						if(isDebug)log.debug("Closing channel: "+channel);
						channel.close();
					} catch (IOException e1) {
						log.error("", e1);
					}
				}
			}
		};

		final AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(new InetSocketAddress(routerOptions.listen_port), acceptListener2, OptionMap.EMPTY);
		server.resumeAccepts();

		if(isInfo)log.info("Listening on " + server.getLocalAddress());
		
		final Thread reaper = new Thread(new Thread(){
			public void run(){
				while(true){
					try{
						Thread.sleep(2000);
						final Iterator<ReadListener> iter = readListeners.iterator();
						while(iter.hasNext()){
							ReadListener listener = iter.next();
							if(!listener.checkLiveness()){
								iter.remove();
							}else{
								//if(isDebug)log.debug(listener.toString());
							}
						}
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}
		});
		reaper.setName("Idle Connection Reaper");
		reaper.start();
		
		int acceptedLast = totalAccepted.get();
		long clientToBackendLast = globalBackendWriteBytes.get();
		long backendToClientLast = globalClientWriteBytes.get();
		long start = System.currentTimeMillis();
		String lastFormatted="";
		while(true){
			Thread.sleep(2000);
			long slept = System.currentTimeMillis() - start;
			double acceptedPerSec = ((double)totalAccepted.get() - (double)acceptedLast) / ((double)slept/1000d);
			double clientToBackendPerSec = (globalBackendWriteBytes.get() - clientToBackendLast) / ((double)slept/1000d);
			double backendToClientPerSec = (globalClientWriteBytes.get() - backendToClientLast) / ((double)slept/1000d);
			final String formatted = String.format("\tStats: %,.1f sess/sec, %,d total sess, %,d curr sess, %,d FR -> %,d BW, %,d FW <- %,d BR, in %,.0f bytes/sec, out %,.0f bytes/sec", 
					acceptedPerSec, totalAccepted.get(), sessionsCount.get(), globalClientReadBytes.get(), globalBackendWriteBytes.get(), globalClientWriteBytes.get(), globalBackendReadBytes.get(), clientToBackendPerSec, backendToClientPerSec);
			if(!formatted.equals(lastFormatted)){
				System.out.println(formatted);
			}
			lastFormatted = formatted;
			acceptedLast = totalAccepted.get();
			clientToBackendLast = globalBackendWriteBytes.get();
			backendToClientLast = globalClientWriteBytes.get();
			start = System.currentTimeMillis();
		}
	}

	private final static class ReadListener implements ChannelListener<ConduitStreamSourceChannel> {
		final static Logger log = Logger.getLogger(ReadListener.class);
		
		private long lastActivity = System.currentTimeMillis();
		private final ByteBuffer buffer = pool.allocate();

		private boolean initHeaders=true;
		private boolean writeHeaders=false;
		private boolean isProxy=false;
		private boolean writeSSLHeadersFromClient=false;

		private volatile boolean allClosed=false;
		private IoFuture<StreamConnection> future;
		private StreamConnection streamConnection;
		private WriteListener writeListener;
		private long totalWritesToBackend=0;
		private long totalWritesToFrontend=0;
		private long totalReadsFromBackend=0;
		private long totalReadsFromFrontend=0;

		private ReadListener(){
			buffer.clear();
		}
		
		@Override
		public final void handleEvent(final ConduitStreamSourceChannel frontendChannel) {
			frontendChannel.suspendReads();
			if(isDebug)MDC.put("channel", streamConnection.hashCode());
			if(!streamConnection.isOpen()|| !frontendChannel.isOpen()){
				if(isDebug)log.debug("Frontend channel is closed.");
				closeAll();
				return;
			}
			if(future != null && future.getStatus().equals(Status.FAILED)){
				if(isDebug)log.debug("Backend connection attempt failed: "+future.getException().getMessage());
				closeAll();
				return;
			}
			try {
				long clientReadBytes=0;
				try{
					if(!initHeaders && buffer.hasRemaining() && !writeSSLHeadersFromClient){
						return;//the buffer still has stuff to write
					}

					buffer.clear();
					clientReadBytes = frontendChannel.read(buffer);
					if(clientReadBytes == -1){
						if(isDebug)log.debug("Client End of stream.");
						closeAll();
						return;
					}
					
					globalClientReadBytes.addAndGet(clientReadBytes);
					buffer.flip();
					if(isDebug){
						totalReadsFromFrontend += clientReadBytes;
						log.debug(buffer.toString());
						log.debug("Read "+clientReadBytes+" bytes from frontend (source)");
					}
					if(initHeaders){
						if(isDebug)log.debug("Suspending reads on frontend (source)");
						frontendChannel.suspendReads();
						initHeaders(frontendChannel, buffer);
					}else if(future != null){
						if(writeSSLHeadersFromClient){
							writeSSLHeadersFromClient=false;
						}
						if(isDebug){
							log.debug("Suspending reads on frontend (source)");
							log.debug("Resuming writes on Backend (sink)");
						}
						frontendChannel.suspendReads();
						future.get().getSinkChannel().resumeWrites();
						
					}
				}catch(IOException e){
					if("Connection reset by peer".equals(e.getMessage())){
						log.info("Connection reset by Frontend (source): "+((InetSocketAddress)streamConnection.getPeerAddress()).toString());
					}else{
						log.error("Error reading from Frontend (source): "+((InetSocketAddress)streamConnection.getPeerAddress()).toString(), e);
					}
					closeAll();
					return;
				}
				lastActivity = System.currentTimeMillis();
			} catch (Exception e) {
				log.error("", e);
				closeAll();
			} finally {
				if(isInfo)MDC.remove("channel");
			}
		}

		public final boolean checkLiveness() {
			if(!streamConnection.isOpen()){
				if(isDebug)log.debug("Frontend channel is closed.");
				closeAll();
				return false;
			}
			if(!streamConnection.getSinkChannel().isOpen()){
				if(isDebug)log.debug("Frontend sink channel is closed.");
				closeAll();
				return false;
			}
			if(!streamConnection.getSourceChannel().isOpen()){
				if(isDebug)log.debug("Frontend source channel is closed.");
				closeAll();
				return false;
			}
			if(future != null){
				if(future.getStatus().equals(Status.FAILED)){
					if(isDebug)log.debug("Backend connection failed.");
					closeAll();
					return false;
				}else if(future.getStatus().equals(Status.DONE)){
					try{
						if(!future.get().isOpen()){
							if(isDebug)log.debug("Backend channel is closed.");
							closeAll();
							return false;
						}
						if(!future.get().getSinkChannel().isOpen()){
							if(isDebug)log.debug("Backend sink channel is closed.");
							closeAll();
							return false;
						}
						if(!future.get().getSourceChannel().isOpen()){
							if(isDebug)log.debug("Backend source channel is closed.");
							closeAll();
							return false;
						}
					}catch(IOException e){
						log.error("", e);
						closeAll();
						return false;
					}
				}
			}
			return true;
		}

		private final void writeProxyHeaders(final ByteBuffer buffer) {
			try{
				final String PROXY_HEADER = "HTTP/1.1 200 OK\r\nProxy-agent: Netscape-Proxy/1.1\r\n\r\n";
				final ByteBuffer okBuff = ByteBuffer.allocate(PROXY_HEADER.getBytes().length);
				okBuff.put(PROXY_HEADER.getBytes());
				okBuff.flip();
				int written = streamConnection.getSinkChannel().write(okBuff);
				//globalClientWriteBytes.addAndGet(written);
				//totalWritesToFrontend += written;
				
				boolean flushed = streamConnection.getSinkChannel().flush();
				if(isDebug)log.debug("Wrote HTTP/1.1 200 OK to frontend (sink) (flushed: "+flushed+")");
				writeHeaders = false;
				buffer.clear();
				isProxy=false;
			}catch(IOException e){
				log.error("Error writing Proxy header to Frontend (sink) "+((InetSocketAddress)streamConnection.getPeerAddress()).toString(), e);
				closeAll();
			}
		}

		private final void initHeaders(final ConduitStreamSourceChannel frontendChannel, final ByteBuffer buffer) {
			if(isDebug){
				log.debug("Suspending writes on frontend (sink)");
				log.debug("Reading (HTTP) headers...");
			}
			streamConnection.getSinkChannel().suspendWrites();//start out suspended, as soon as the backend source channel is ready, we will get resumed
			final String header = StandardCharsets.UTF_8.decode(buffer).toString();
			buffer.rewind();
			final String[] lines = header.toString().split("\r\n");
			//if(isDebug)log.debug(lines[0]);
			//for(String line: lines){
			//	System.out.println(line);
			//}
			
			String host="";
			int port = -1;
			final String[] parts = lines[0].split(" ");
			if(parts.length == 3){
				if("CONNECT".equals(parts[0])){//Http Proxy Request
					host = parts[1].substring(0, parts[1].indexOf(":"));
					port = Integer.parseInt(parts[1].substring(parts[1].indexOf(':')+1, parts[1].length()));
					isProxy=true;
					totalReadsFromFrontend -= buffer.limit();//subtract proxy header counts
					globalClientReadBytes.addAndGet(buffer.limit()*-1);
				}else if("HTTP/1.1".equals(parts[2]) || "HTTP/1.0".equals(parts[2])){
					String hostLine = lines[1];
					for(int i=0;i<lines.length;i++){
						if(lines[i].startsWith("Host:")){
							hostLine = lines[i];
							break;
						}
					}
					final String[] hostLineParts = hostLine.split(" ");
					if(hostLineParts.length == 2){
						if(hostLineParts[1].indexOf(":") > 0){
							String[] hostPort = hostLineParts[1].split(":");
							host = hostPort[0];
							port = Integer.parseInt(hostPort[1]);
						}else{
							host = hostLineParts[1];
							port = 80;
						}
					}else{
						log.warn("Unknown Protocol: "+lines[0]);
						this.closeAll();
						return;
					}
				}
			}else{
				log.warn("Unknown Protocol: "+lines[0]);
				this.closeAll();
				return;
			}
			
			if("127.0.0.1".equals(host) || "localhost".equals(host) || "192.168.1.133".equals(host) || "192.168.1.150".equals(host) || "192.168.1.218".equals(host)){
				host = routerOptions.backend_host;
				port = routerOptions.backend_port;
			}
			
			final InetSocketAddress addr = new InetSocketAddress(host, port);
			if(addr.isUnresolved()){
				log.error("Could not resolve address: "+host);
				closeAll();
				return;
			}
			if(isInfo)log.info("Session Established. Frontend: "+((InetSocketAddress)streamConnection.getPeerAddress()).toString()+", Backend: "+addr);
			future = worker.openStreamConnection(addr, backendConnection -> {
					if(isProxy){
						writeProxyHeaders(buffer);
						this.initHeaders=false;
						this.writeHeaders=false;
						writeSSLHeadersFromClient=true;
					}
					if(isDebug)log.debug("Resuming reads on frontend (source)");
					frontendChannel.wakeupReads();
					backendConnection.setCloseListener(backendChannel2 -> {
						if(isDebug)log.debug("Backend channel closed.");
						streamConnection.getSinkChannel().resumeWrites();//resume writes to client
						closeAll();
					});
					backendConnection.getSourceChannel().getReadSetter().set(backendSource -> {
						if(isDebug)log.debug("Resuming writes on frontend (sink)");//TODO why is this called multiple times in succession
						streamConnection.getSinkChannel().resumeWrites();//resume writes to client
					});
					backendConnection.getSourceChannel().setCloseListener(backendSource -> {
						closeAll();
					});
					backendConnection.getSinkChannel().setCloseListener(backendSink -> {
						closeAll();
					});
					backendConnection.getSinkChannel().getWriteSetter().set(backendSink -> {
						if(writeSSLHeadersFromClient){
							frontendChannel.resumeReads();
							backendSink.suspendWrites();
							return;
						}
						if(isDebug)log.debug("Suspending reads on frontend (source)");
						streamConnection.getSourceChannel().suspendReads();
						try {
							if(!initHeaders && writeHeaders){
								writeHeaders(buffer);
								if(isDebug)log.debug("Resuming writes on Backend (sink)");
								future.get().getSinkChannel().resumeWrites();
								return;
							}
							writeBody(buffer, backendSink);
						} catch (IOException e) {
							closeAll();
							return;
						}
					});
					backendConnection.getSourceChannel().resumeReads();
					backendConnection.getSinkChannel().resumeWrites();
			}, xnioOptions);
			this.initHeaders=false;
			this.writeHeaders=true;
		}

		private final void writeHeaders(final ByteBuffer buffer) throws IOException {
			try{
				int remaining = buffer.remaining();
				if(remaining == 0){
					if(isDebug)log.debug("Suspending writes on Backend (sink)");
					streamConnection.getSinkChannel().suspendWrites();
					if(isDebug)log.debug("Resuming reads on Frontend (source)");
					streamConnection.getSourceChannel().resumeReads();
					return;
				}
				int totalWritten=0;
				int backendWriteBytes=0;
				if(isDebug)log.debug(buffer.toString());
				//System.out.println(Buffers.createDumper(buffer, 1, 4).toString());
				final ConduitStreamSinkChannel sink = future.get().getSinkChannel();
				while((backendWriteBytes = sink.write(buffer)) > 0l){//write to backend
					boolean flushed = sink.flush();
					totalWritten += backendWriteBytes;
					if(isDebug)log.debug("Wrote "+backendWriteBytes+" header bytes to Backend (flushed: "+flushed+") Remaining: "+(remaining - totalWritten));
				}
				globalBackendWriteBytes.addAndGet(totalWritten);
				this.totalWritesToBackend += totalWritten;
				if(remaining != totalWritten){
					if(isDebug)log.debug("Header bytes remaining: "+(remaining - totalWritten));
				}else{
					this.writeHeaders=false;
					if(isDebug)log.debug("Resuming reads on Frontend (source)");
					streamConnection.getSourceChannel().resumeReads();
				}
			}catch(IOException e){
				if("Broken pipe".equals(e.getMessage())){
					log.error("Error writing header to Backend (sink) "+((InetSocketAddress)future.get().getPeerAddress()).toString()+": Broken pipe");
				}else{
					log.error("Error writing header to Backend (sink) "+((InetSocketAddress)future.get().getPeerAddress()).toString(), e);
				}
				closeAll();
			}
		}
		
		private final void writeBody(final ByteBuffer buffer, ConduitStreamSinkChannel channel) throws IOException {
			if(isInfo)MDC.put("channel", streamConnection.hashCode());
			if(allClosed){
				return;
			}
			try{
				//System.out.println(Buffers.createDumper(buffer, 1, 4).toString());
				
				if(!channel.isOpen()){
					if(isDebug)log.debug("Backend sink is closed");
					closeAll();
					return;
				}
				int remaining = buffer.remaining();
				if(remaining == 0){
					if(isDebug)log.debug("Suspending writes on Backend (sink)");
					channel.suspendWrites();
					if(isDebug)log.debug("Resuming reads on Frontend (source)");
					streamConnection.getSourceChannel().resumeReads();
					return;
				}
				if(!streamConnection.isOpen()|| !streamConnection.getSourceChannel().isOpen() || !streamConnection.getSinkChannel().isOpen()){
					if(isDebug)log.debug("Frontend channel is closed.");
					closeAll();
					return;
				}
				int totalBackendWriteBytes=0;
				int backendWriteBytes=0;
				
				//final String contents = StandardCharsets.US_ASCII.decode(buffer).toString();
				//System.out.println(contents);
				//buffer.rewind();

				if(isDebug)log.debug(buffer.toString());
				while((backendWriteBytes = channel.write(buffer)) > 0){
					boolean flushed = channel.flush();
					totalBackendWriteBytes+=backendWriteBytes;
					if(isDebug)log.debug("Wrote "+backendWriteBytes+" non-header bytes to Backend (flushed: "+flushed+"). Remaining: "+(remaining-backendWriteBytes));
					totalWritesToBackend += backendWriteBytes;
				}
				globalBackendWriteBytes.addAndGet(totalBackendWriteBytes);
				if(totalBackendWriteBytes != remaining){
					if(isDebug)log.debug("Suspending reads on Frontend (source)");
					streamConnection.getSourceChannel().suspendReads();
					channel.resumeWrites();
				}else{
					if(totalReadsFromFrontend != totalWritesToBackend){
						if(isDebug)log.debug("Discrepancy totalReadsFromFrontend != totalWritesToBackend: "+totalReadsFromFrontend+" : "+totalWritesToBackend);
					}
					if(writeSSLHeadersFromClient){
						writeSSLHeadersFromClient=false;
					}
					if(isDebug)log.debug("Suspending writes on Backend (sink)");
					channel.suspendWrites();
					if(isDebug)log.debug("Resuming reads on Frontend (source)");
					streamConnection.getSourceChannel().resumeReads();
				}
			}catch(IOException e){
				log.error("Error writing body to Backend (sink) "+((InetSocketAddress)future.get().getPeerAddress()).toString(), e);
				closeAll();
			}finally{
				MDC.remove("channel");
			}
		}

		private final void closeAll() {
			if(allClosed){
				return;
			}
			allClosed=true;
			sessionsCount.decrementAndGet();
			if(isDebug)log.debug("Closing all resources.");
			try{
				streamConnection.close();
			}catch(IOException e1){
				log.error("", e1);
			}finally{
				try {
					if(future != null){
						if(!future.getStatus().equals(Status.FAILED)){
							future.get().close();
						}
					}
				} catch (CancellationException | IOException e1) {
					log.error("", e1);
				}
				ByteBufferPool.free(buffer);
				ByteBufferPool.free(writeListener.buffer);
			}
			if(isInfo){
				String backendAddr = "";
				if(null != future){
					try {
						backendAddr = ((InetSocketAddress)future.get().getPeerAddress()).toString();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				log.info(String.format("Session Closed. Frontend: "+((InetSocketAddress)streamConnection.getPeerAddress()).toString()+", Backend: "+backendAddr+" Stats: %,d writes to backend, %,d reads from frontend, %,d writes to frontend, %,d reads from backend", totalWritesToBackend, totalReadsFromFrontend, totalWritesToFrontend, totalReadsFromBackend));
			}
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("ReadListener [lastActivity=").append(lastActivity).append(", buffer=")
					.append(buffer).append(", initHeaders=").append(initHeaders).append(", writeHeaders=")
					.append(writeHeaders).append(", allClosed=").append(allClosed).append(", future=").append(future)
					.append(", streamConnection=").append(streamConnection).append(", writeListener=")
					.append(writeListener).append(", totalWritesToBackend=").append(totalWritesToBackend)
					.append(", totalWritesToFrontend=").append(totalWritesToFrontend).append(", totalReadsFromBackend=")
					.append(totalReadsFromBackend).append(", totalReadsFromFrontend=").append(totalReadsFromFrontend)
					.append(", isProxy=").append(isProxy);
			builder.append(", frontent source resumed? ").append(streamConnection.getSinkChannel().isWriteResumed());
			builder.append(", frontent sink resumed? ").append(streamConnection.getSinkChannel().isWriteResumed());
			if(future != null){
				if(future.getStatus().equals(Status.DONE)){
					try {
						builder.append(", backend sink resumed? ").append(future.get().getSinkChannel().isWriteResumed());
						builder.append(", backend source resumed? ").append(future.get().getSourceChannel().isReadResumed());
					} catch (CancellationException | IOException e) {
						e.printStackTrace();
					}
				}
			}
			builder.append("]");
			return builder.toString();
		}
	}

	public final static class WriteListener implements ChannelListener<ConduitStreamSinkChannel> {
		private final ByteBuffer buffer = pool.allocate();
		private ReadListener readListener;

		public WriteListener(ReadListener readListener){
			this.readListener = readListener;
		}
		
		@Override
		public final void handleEvent(final ConduitStreamSinkChannel channel) {
			if(readListener.initHeaders || readListener.writeHeaders || readListener.allClosed){
				return;
			}
			if(isInfo)MDC.put("channel", readListener.streamConnection.hashCode());
			if(!readListener.streamConnection.isOpen() || !channel.isOpen()){
				if(isDebug)log.debug("Frontend channel is closed.");
				readListener.closeAll();
				return;
			}
			if(!channel.isOpen()){
				if(isDebug)log.debug("Frontent sink is closed.");
				readListener.closeAll();
				return;
			}
			try {
				if(!readListener.future.get().isOpen()){
					if(isDebug)log.debug("Backend channel is closed.");
					readListener.closeAll();
					return;
				}
				if(!readListener.future.get().getSourceChannel().isOpen()){
					if(isDebug)log.debug("Backend source channel is closed.");
					readListener.closeAll();
					return;
				}
				int res=0;

				buffer.clear();
				try{
					while((res = readListener.future.get().getSourceChannel().read(buffer)) > 0){
						readListener.totalReadsFromBackend += res;
						globalBackendReadBytes.addAndGet(res);
						buffer.flip();
						try{
							long count = channel.write(buffer);
							boolean flushed = channel.flush();
							if(isDebug)log.debug("Read "+res+" bytes from Backend (source)");
							if(isDebug)log.debug("Wrote "+count+" bytes from backend to client (flushed: "+flushed+")");
							readListener.totalWritesToFrontend += count;
							globalClientWriteBytes.addAndGet(count);
							if(count != res){
								System.out.println("count != res -> "+count+" "+res);
								return;
							}
						}catch(IOException e){
							log.error("Error writing to Frontend (sink) "+((InetSocketAddress)readListener.streamConnection.getPeerAddress()).toString(),e);
							readListener.closeAll();
							return;
						}
					}
					if(res == -1){
						if(isDebug)log.debug("Backend End of stream.");
						channel.flush();
						readListener.closeAll();
						return;
					}else{
						readListener.streamConnection.getSinkChannel().suspendWrites();
						if(isDebug)log.debug("Suspending writes on Frontend (sink)");
						readListener.lastActivity = System.currentTimeMillis();
					}
				}catch(IOException e){
					if("Connection reset by peer".equals(e.getMessage())){
						log.error("Connection reset by Backend (source) "+((InetSocketAddress)readListener.future.get().getPeerAddress()).toString());
					}else{
						log.error("Error reading from Backend (source) "+((InetSocketAddress)readListener.future.get().getPeerAddress()).toString(),e);
					}
					readListener.closeAll();
					return;
				}
			} catch (IOException e) {
				log.error("", e);
				readListener.closeAll();
				return;
			}finally{
				//if(isDebug)log.debug("Suspending writes on front-end (sink)");
				if(isInfo)MDC.remove("channel");;
				//channel.suspendWrites();
			}
			
		}
	}
	
	public final static class Options {
		@Option(name = "-listen_port", usage="port")
		public Integer listen_port = 7080;
		
		@Option(name = "-backend_host", usage="host")
		public String backend_host = "192.168.1.150";
		
		@Option(name = "-backend_port", usage="port")
		public Integer backend_port = 80;

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Options: router_port=").append(listen_port).append(", backend_host=").append(backend_host)
					.append(", backend_port=").append(backend_port).append("");
			return builder.toString();
		}
	}

}