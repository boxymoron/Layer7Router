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
			.set(org.xnio.Options.READ_TIMEOUT, 30000)
			.set(org.xnio.Options.WRITE_TIMEOUT, 30000)
			.set(org.xnio.Options.USE_DIRECT_BUFFERS, true)
			.set(org.xnio.Options.WORKER_IO_THREADS, 2)
			.set(org.xnio.Options.SPLIT_READ_WRITE_THREADS, false)
			.set(org.xnio.Options.BACKLOG, 8192)
			.set(org.xnio.Options.KEEP_ALIVE, true)
			.getMap();
	static XnioWorker worker;
	
	final static AtomicInteger totalAccepted = new AtomicInteger();
	final static AtomicInteger listenersCount = new AtomicInteger();
	final static AtomicInteger writersCount = new AtomicInteger();	
	final static AtomicLong clientToBackendBytes = new AtomicLong();
	final static AtomicLong backendToClientBytes = new AtomicLong();

	static ByteBufferSlicePool pool = new ByteBufferSlicePool(1024*8, 32*1024*1024*32);
	
	public static Options routerOptions = new Options();
	
	final static boolean isInfo=log.isInfoEnabled();
	final static boolean isDebug=log.isDebugEnabled();
	final static boolean isTrace=log.isTraceEnabled();

	public static void main(String[] args) throws Exception {
		final CmdLineParser cmdLineParser = new CmdLineParser(routerOptions);
		cmdLineParser.parseArgument(args);
		System.out.println(routerOptions.toString());

		worker = xnio.createWorker(xnioOptions);
		
		final Deque<ReadListener> readListeners = new ConcurrentLinkedDeque();

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
						listenersCount.incrementAndGet();
						
						final WriteListener writeListener = new WriteListener(readListener);
						accepted.getSinkChannel().setWriteListener(writeListener);
						accepted.getSinkChannel().setCloseListener(writeListener);
						readListener.writeListener = writeListener;
						writersCount.incrementAndGet();

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

		final AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(new InetSocketAddress(routerOptions.router_port), acceptListener2, OptionMap.EMPTY);
		server.resumeAccepts();

		if(isInfo)log.info("Listening on " + server.getLocalAddress());
		
		final Thread reaper = new Thread(new Thread(){
			public void run(){
				while(true){
					try{
						Thread.sleep(10000);
						final Iterator<ReadListener> iter = readListeners.iterator();
						while(iter.hasNext()){
							if(!iter.next().checkLiveness()){
								iter.remove();
							};
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
		long clientToBackendLast = clientToBackendBytes.get();
		long backendToClientLast = backendToClientBytes.get();
		long start = System.currentTimeMillis();
		String lastFormatted="";
		while(true){
			Thread.sleep(2000);
			long slept = System.currentTimeMillis() - start;
			double acceptedPerSec = ((double)totalAccepted.get() - (double)acceptedLast) / ((double)slept/1000d);
			double clientToBackendPerSec = (clientToBackendBytes.get() - clientToBackendLast) / ((double)slept/1000d);
			double backendToClientPerSec = (backendToClientBytes.get() - backendToClientLast) / ((double)slept/1000d);
			final String formatted = String.format("\tStats: %,.2f accepts/sec, %,d total accepts, %,d listeners, %,d writers, in %,d bytes, out %,d bytes, in %,.0f bytes/sec, out %,.0f bytes/sec", 
					acceptedPerSec, totalAccepted.get(), listenersCount.get(), writersCount.get(), clientToBackendBytes.get(), backendToClientBytes.get(), clientToBackendPerSec, backendToClientPerSec);
			if(!formatted.equals(lastFormatted)){
				System.out.println(formatted);
			}
			lastFormatted = formatted;
			acceptedLast = totalAccepted.get();
			clientToBackendLast = clientToBackendBytes.get();
			backendToClientLast = backendToClientBytes.get();
			start = System.currentTimeMillis();
		}
	}

	private final static class ReadListener implements ChannelListener<ConduitStreamSourceChannel> {
		final static Logger log = Logger.getLogger(ReadListener.class);
		
		private AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());
		private final Pooled<ByteBuffer> pooledBuffer = pool.allocate();

		private boolean initHeaders=true;
		private boolean writeHeaders=false;

		private volatile boolean allClosed=false;
		private IoFuture<StreamConnection> future;
		private StreamConnection streamConnection;
		private WriteListener writeListener;
		private ReadListener(){}
		private long totalReads=0;
		private long totalWrites=0;
		private boolean isProxy=false;
		
		@Override
		public final void handleEvent(final ConduitStreamSourceChannel frontendChannel) {
			if(isInfo)MDC.put("channel", streamConnection.hashCode());
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
				final ByteBuffer buffer = pooledBuffer.getResource();
				
				if(future != null){
					if(isDebug)log.debug("Suspending writes on backend (sink)");
					future.get().getSinkChannel().suspendWrites();
				}
				if(!initHeaders && writeHeaders){
					if(isProxy){
						writeProxyHeaders(buffer);
						return;
					}else{
						writeHeaders(buffer);
					}
				}

				long clientReadBytes=0;
				try{
					while ((clientReadBytes = frontendChannel.read(buffer)) > 0) {
						buffer.flip();
						if(isDebug)log.debug(buffer.toString());
						if(initHeaders){
							if(isDebug)log.debug("Suspending reads on frontend (source)");
							frontendChannel.suspendReads();
							initHeaders(frontendChannel, buffer);
							return;
						}else{
							writeBody(buffer);
						}
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

				if(clientReadBytes == -1){
					if(isDebug)log.debug("Client End of stream.");
					closeAll();
				}else{
					lastActivity.set(System.currentTimeMillis());
				}
			} catch (IOException e) {
				log.error("", e);
				closeAll();
			} finally {
				if(isInfo)MDC.remove("channel");
			}
		}

		public final boolean checkLiveness() {
			//System.out.println("checking frontend aliveness: "+streamConnection.getPeerAddress());
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
						//System.out.println("checking backend aliveness: "+future.get().getPeerAddress());
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
				ByteBuffer okBuff = ByteBuffer.allocate("HTTP/1.1 200 OK\r\nProxy-agent: Netscape-Proxy/1.1\r\n\r\n".getBytes().length);
				okBuff.put("HTTP/1.1 200 OK\r\nProxy-agent: Netscape-Proxy/1.1\r\n\r\n".getBytes());
				okBuff.flip();
				streamConnection.getSinkChannel().write(okBuff);
				boolean flushed = streamConnection.getSinkChannel().flush();
				if(isDebug)log.debug("Wrote HTTP/1.1 200 OK to frontent (sink) (flushed: "+flushed+")");
				writeHeaders= false;
				buffer.clear();
				streamConnection.getSourceChannel().wakeupReads();
				streamConnection.getSinkChannel().wakeupWrites();
			}catch(IOException e){
				log.error("Error writing Proxy header to Frontend (sink) "+((InetSocketAddress)streamConnection.getPeerAddress()).toString(), e);
				closeAll();
			}
		}

		private final void initHeaders(final ConduitStreamSourceChannel frontendChannel, final ByteBuffer buffer) {
			if(isDebug)log.debug("Read "+buffer.limit()+" header bytes");
			if(isDebug)log.debug("Suspending writes on frontend (sink)");
			streamConnection.getSinkChannel().suspendWrites();//start out suspended, as soon as the backend source channel is ready, we will get resumed
			if(isDebug)log.debug("Reading (HTTP) headers...");
			String header = StandardCharsets.UTF_8.decode(buffer).toString();
			String[] lines = header.toString().split("\r\n");
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
			
			buffer.rewind();
			
			final InetSocketAddress addr = new InetSocketAddress(host, port);
			if(addr.isUnresolved()){
				log.error("Could not resolve address: "+host);
				closeAll();
				return;
			}
			log.info("TCP Session Established. Frontend: "+((InetSocketAddress)streamConnection.getPeerAddress()).toString()+", Backend: "+addr);
			future = worker.openStreamConnection(addr, backendChannel -> {
					if(isDebug)log.debug("Resuming reads on frontend (source)");
					frontendChannel.wakeupReads();
					backendChannel.setCloseListener(backendChannel2 -> {
						if(isDebug)log.debug("Backend channel closed.");
						streamConnection.getSinkChannel().resumeWrites();//resume writes to client
						closeAll();
					});
					backendChannel.getSourceChannel().getReadSetter().set(channel2 ->{
						//if(isDebug)log.debug("Resuming writes on frontend (sink)");//TODO why is this called multiple times in succession
						streamConnection.getSinkChannel().resumeWrites();//resume writes to client
					});
					backendChannel.getSinkChannel().getWriteSetter().set(channel2 ->{
						//if(isDebug)log.debug("Resuming reads on frontend (source)");//TODO why is this called multiple times in succession
						frontendChannel.resumeReads();
					});
					backendChannel.getSourceChannel().resumeReads();
					backendChannel.getSinkChannel().resumeWrites();
					
			}, xnioOptions);
			this.initHeaders=false;
			this.writeHeaders=true;
		}

		private final void writeHeaders(final ByteBuffer buffer) throws IOException {
			try{
				int totalWritten=0;
				int backendWriteBytes=0;
				if(isDebug)log.debug(buffer.toString());
				//System.out.println(Buffers.createDumper(buffer, 1, 4).toString());
				final ConduitStreamSinkChannel sink = future.get().getSinkChannel();
				while((backendWriteBytes = sink.write(buffer)) > 0l){//write to backend
					boolean flushed = sink.flush();
					if(isInfo)if(isDebug)log.debug("Wrote "+backendWriteBytes+" header bytes to Backend (flushed: "+flushed+")");
					totalWritten += backendWriteBytes;
				}
				clientToBackendBytes.addAndGet(totalWritten);
				totalReads += totalWritten;
				buffer.clear();
				this.writeHeaders=false;
			}catch(IOException e){
				if("Broken pipe".equals(e.getMessage())){
					log.error("Error writing header to Backend (sink) "+((InetSocketAddress)future.get().getPeerAddress()).toString()+": Broken pipe");
				}else{
					log.error("Error writing header to Backend (sink) "+((InetSocketAddress)future.get().getPeerAddress()).toString(), e);
				}
				closeAll();
			}
		}
		
		private final void writeBody(final ByteBuffer buffer) throws IOException {
			try{
				//System.out.println(Buffers.createDumper(buffer, 1, 4).toString());
				final ConduitStreamSinkChannel sink = future.get().getSinkChannel();
				int totalWritten=0;
				int backendWriteBytes=0;
				while((backendWriteBytes = sink.write(buffer)) > 0l){
					boolean flushed = sink.flush();
					if(isDebug)log.debug("Wrote "+backendWriteBytes+" non-header bytes to Backend (flushed: "+flushed+")");
					totalWritten += backendWriteBytes;
				}
				clientToBackendBytes.addAndGet(totalWritten);
				totalReads += totalWritten;
				buffer.clear();
			}catch(IOException e){
				log.error("Error writing body to Backend (sink) "+((InetSocketAddress)future.get().getPeerAddress()).toString(), e);
				closeAll();
			}
		}

		private final void closeAll() {
			if(allClosed){
				return;
			}
			allClosed=true;
			listenersCount.decrementAndGet();
			writersCount.decrementAndGet();
			pooledBuffer.getResource().clear();
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
				pooledBuffer.free();
				writeListener.pooledBuffer.free();
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
				log.info(String.format("TCP Session Closed. Frontend: "+((InetSocketAddress)streamConnection.getPeerAddress()).toString()+", Backend: "+backendAddr+" Stats: %,d total reads, %,d total writes", totalReads, totalWrites));
			}
		}
	}

	public final static class WriteListener implements ChannelListener<ConduitStreamSinkChannel> {
		private final Pooled<ByteBuffer> pooledBuffer = pool.allocate();
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

				final ByteBuffer buffer = pooledBuffer.getResource();
				buffer.clear();

				try{
					while((res = readListener.future.get().getSourceChannel().read(buffer)) > 0){
						buffer.flip();
						try{
							long count = channel.write(buffer);
							boolean flushed = channel.flush();
							if(isDebug)log.debug("Read "+res+" bytes from frontend (sink)");
							if(isDebug)log.debug("Wrote "+count+" bytes from backend to client (flushed: "+flushed+")");
							readListener.totalWrites += count;
							backendToClientBytes.addAndGet(count);
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
						//readListener.closeAll();
						return;
					}else{
						readListener.lastActivity.set(System.currentTimeMillis());
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
		@Option(name = "-router_port", usage="port")
		public Integer router_port = 7080;
		
		@Option(name = "-backend_host", usage="host")
		public String backend_host = "192.168.1.150";
		
		@Option(name = "-backend_port", usage="port")
		public Integer backend_port = 80;

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Options: router_port=").append(router_port).append(", backend_host=").append(backend_host)
					.append(", backend_port=").append(backend_port).append("");
			return builder.toString();
		}
	}

}