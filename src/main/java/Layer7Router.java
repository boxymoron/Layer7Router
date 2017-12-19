import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.MDC;
import org.jboss.logging.Logger;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.xnio.ByteBufferSlicePool;
import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.OptionMap;

import org.xnio.Pooled;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;

@SuppressWarnings("deprecation")
public final class Layer7Router {

	final static Logger log = Logger.getLogger(Layer7Router.class);

	final static Xnio xnio = Xnio.getInstance();
	final static OptionMap xnioOptions = OptionMap.builder()
			.set(org.xnio.Options.ALLOW_BLOCKING, false)
			.set(org.xnio.Options.RECEIVE_BUFFER, 1024*32)
			.set(org.xnio.Options.SEND_BUFFER, 1024*32)
			.set(org.xnio.Options.READ_TIMEOUT, 30000)
			.set(org.xnio.Options.WRITE_TIMEOUT, 30000)
			.set(org.xnio.Options.USE_DIRECT_BUFFERS, true)
			.set(org.xnio.Options.WORKER_IO_THREADS, 4)
			.set(org.xnio.Options.SPLIT_READ_WRITE_THREADS, true)
			.set(org.xnio.Options.BACKLOG, 8192)
			.getMap();
	static XnioWorker worker;
	
	final static AtomicInteger totalAccepted = new AtomicInteger();
	final static AtomicInteger listenersCount = new AtomicInteger();
	final static AtomicInteger writersCount = new AtomicInteger();	
	final static AtomicLong clientToBackendBytes = new AtomicLong();
	final static AtomicLong backendToClientBytes = new AtomicLong();

	static ByteBufferSlicePool pool = new ByteBufferSlicePool(1024*32, 32*1024*1024*32);//allocate 32K buffers of 32KB each
	
	public static Options routerOptions = new Options();

	public static void main(String[] args) throws Exception {
		final CmdLineParser cmdLineParser = new CmdLineParser(routerOptions);
		cmdLineParser.parseArgument(args);
		System.out.println(routerOptions.toString());
		
		worker = xnio.createWorker(xnioOptions);

		final ChannelListener<AcceptingChannel<StreamConnection>> acceptListener2 = new ChannelListener<AcceptingChannel<StreamConnection>>() {
			@Override
			public final void handleEvent(AcceptingChannel<StreamConnection> channel) {
				try {
					StreamConnection accepted;
					while ((accepted = channel.accept()) != null) {
						if(log.isInfoEnabled())log.info("Accepted: " + accepted.getPeerAddress());
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
					}
				} catch (IOException e) {
					e.printStackTrace();
					try {
						if(log.isInfoEnabled())log.info("Closing channel: "+channel);
						channel.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			}
		};

		final AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(new InetSocketAddress(routerOptions.router_port), acceptListener2, OptionMap.EMPTY);
		server.resumeAccepts();

		if(log.isInfoEnabled())log.info("Listening on " + server.getLocalAddress());
		int acceptedLast = totalAccepted.get();
		long clientToBackendLast = clientToBackendBytes.get();
		long backendToClientLast = backendToClientBytes.get();
		long start = System.currentTimeMillis();
		String lastFormatted="";
		while(true){
			Thread.sleep(1000);
			long slept = System.currentTimeMillis() - start;
			double acceptedPerSec = ((double)totalAccepted.get() - (double)acceptedLast) / ((double)slept/1000d);
			long clientToBackendPerSec = (clientToBackendBytes.get() - clientToBackendLast) / (slept/1000);
			long backendToClientPerSec = (backendToClientBytes.get() - backendToClientLast) / (slept/1000);
			final String formatted = String.format("\tStats: %,.2f accepts/sec, %,d total accepts, %,d listeners, %,d writers, in %,d bytes, out %,d bytes, in %,d bytes/sec, out %,d bytes/sec", 
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
			if(log.isInfoEnabled())MDC.put("channel", streamConnection.hashCode());
			if(!streamConnection.isOpen()|| !frontendChannel.isOpen()){
				if(log.isInfoEnabled())log.info("Frontend channel is closed.");
				closeAll();
				return;
			}
			try {
				final ByteBuffer buffer = pooledBuffer.getResource();
				
				if(future != null){
					if(log.isInfoEnabled())log.info("Suspending writes on backend (sink)");
					future.get().getSinkChannel().suspendWrites();
				}
				if(!initHeaders && writeHeaders){
					if(isProxy){
						ByteBuffer okBuff = ByteBuffer.allocate("HTTP/1.1 200 OK\r\nProxy-agent: Netscape-Proxy/1.1\r\n\r\n".getBytes().length);
						okBuff.put("HTTP/1.1 200 OK\r\nProxy-agent: Netscape-Proxy/1.1\r\n\r\n".getBytes());
						okBuff.flip();
						streamConnection.getSinkChannel().write(okBuff);
						boolean flushed = streamConnection.getSinkChannel().flush();
						log.info("Wrote HTTP/1.1 200 OK to frontent (sink) (flushed: "+flushed+")");
						writeHeaders= false;
						buffer.clear();
						streamConnection.getSourceChannel().wakeupReads();
						streamConnection.getSinkChannel().wakeupWrites();
						return;
					}else{
						writeHeaders(buffer);
					}
				}

				long clientReadBytes=0;
				while ((clientReadBytes = frontendChannel.read(buffer)) > 0) {
					buffer.flip();
					if(log.isInfoEnabled())log.info(buffer.toString());
					if(initHeaders){
						if(log.isInfoEnabled())log.info("Suspending reads on frontend (source)");
						frontendChannel.suspendReads();
						initHeaders(frontendChannel, buffer);
						return;
					}else{
						writeBody(buffer);
					}
				}

				if(clientReadBytes == -1){
					if(log.isInfoEnabled())log.info("Client End of stream.");
					closeAll();
				}

			} catch (IOException e) {
				e.printStackTrace();
				closeAll();
			} finally {
				if(log.isInfoEnabled())MDC.remove("channel");
			}
		}

		private void initHeaders(final ConduitStreamSourceChannel frontendChannel, final ByteBuffer buffer) {
			if(log.isInfoEnabled())log.info("Read "+buffer.limit()+" header bytes");
			if(log.isInfoEnabled())log.info("Suspending writes on frontend (sink)");
			streamConnection.getSinkChannel().suspendWrites();//start out suspended, as soon as the backend source channel is ready, we will get resumed
			if(log.isInfoEnabled())log.info("Reading (HTTP) headers...");
			String header = StandardCharsets.UTF_8.decode(buffer).toString();
			String[] lines = header.toString().split("\r\n");
			//if(log.isInfoEnabled())log.info(lines[0]);
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
			
			if("127.0.0.1".equals(host) || "localhost".equals(host) || "192.168.1.133".equals("host") || "192.168.1.150".equals(host)){
				host = routerOptions.backend_host;
				port = routerOptions.backend_port;
			}

			buffer.rewind();
			
			if(log.isInfoEnabled())log.info("Opening TCP connection to backend: "+host+":"+port);
			final InetSocketAddress addr = new InetSocketAddress(host, port);
			future = worker.openStreamConnection(addr, backendChannel -> {
					if(log.isInfoEnabled())log.info("Resuming reads on frontend (source)");
					frontendChannel.wakeupReads();
					backendChannel.setCloseListener(backendChannel2 -> {
						if(log.isInfoEnabled())log.info("Backend channel closed.");
						streamConnection.getSinkChannel().resumeWrites();//resume writes to client
						closeAll();
					});
					backendChannel.getSourceChannel().getReadSetter().set(channel2 ->{
						//if(log.isInfoEnabled())log.info("Resuming writes on frontend (sink)");//TODO why is this called multiple times in succession
						streamConnection.getSinkChannel().resumeWrites();//resume writes to client
					});
					backendChannel.getSinkChannel().getWriteSetter().set(channel2 ->{
						//if(log.isInfoEnabled())log.info("Resuming reads on frontend (source)");//TODO why is this called multiple times in succession
						frontendChannel.resumeReads();
					});
					backendChannel.getSourceChannel().resumeReads();
					backendChannel.getSinkChannel().resumeWrites();
					
			}, xnioOptions);
			this.initHeaders=false;
			this.writeHeaders=true;
		}

		private int writeHeaders(final ByteBuffer buffer) throws IOException {
			int totalWritten=0;
			int backendWriteBytes=0;
			if(log.isInfoEnabled())log.info(buffer.toString());
			//System.out.println(Buffers.createDumper(buffer, 1, 4).toString());
			final ConduitStreamSinkChannel sink = future.get().getSinkChannel();
			while((backendWriteBytes = sink.write(buffer)) > 0l){//write to backend
				boolean flushed = sink.flush();
				if(log.isInfoEnabled())if(log.isInfoEnabled())log.info("Wrote "+backendWriteBytes+" header bytes to backend (flushed: "+flushed+")");
				totalWritten += backendWriteBytes;
			}
			clientToBackendBytes.addAndGet(totalWritten);
			totalReads += totalWritten;
			buffer.clear();
			this.writeHeaders=false;
			return totalWritten;
		}
		
		private int writeBody(final ByteBuffer buffer) throws IOException {
			//System.out.println(Buffers.createDumper(buffer, 1, 4).toString());
			final ConduitStreamSinkChannel sink = future.get().getSinkChannel();
			int totalWritten=0;
			int backendWriteBytes=0;
			while((backendWriteBytes = sink.write(buffer)) > 0l){
				boolean flushed = sink.flush();
				if(log.isInfoEnabled())log.info("Wrote "+backendWriteBytes+" non-header bytes to backend (flushed: "+flushed+")");
				totalWritten += backendWriteBytes;
			}
			clientToBackendBytes.addAndGet(totalWritten);
			totalReads += totalWritten;
			buffer.clear();
			return totalWritten;
		}

		private void closeAll() {
			if(allClosed){
				return;
			}
			allClosed=true;
			listenersCount.decrementAndGet();
			writersCount.decrementAndGet();
			pooledBuffer.getResource().clear();
			if(log.isInfoEnabled())log.info("Closing all resources.");
			try{
				streamConnection.close();
			}catch(IOException e1){
				e1.printStackTrace();
			}finally{
				try {
					if(future != null){
						future.get().close();
					}
				} catch (CancellationException | IOException e1) {
					e1.printStackTrace();
				}
				pooledBuffer.free();
				writeListener.pooledBuffer.free();
			}
			if(log.isInfoEnabled())log.info(String.format("TCP Session stats: %,d total reads, %,d total writes", totalReads, totalWrites));
		}
	}

	public final static class WriteListener implements ChannelListener<ConduitStreamSinkChannel> {
		private final Pooled<ByteBuffer> pooledBuffer = pool.allocate();
		private ReadListener readListener;

		public WriteListener(ReadListener readListener){
			this.readListener = readListener;
		}
		
		@Override
		public void handleEvent(final ConduitStreamSinkChannel channel) {
			if(readListener.initHeaders || readListener.writeHeaders || readListener.allClosed){
				return;
			}
			if(log.isInfoEnabled())MDC.put("channel", readListener.streamConnection.hashCode());
			if(!readListener.streamConnection.isOpen() || !channel.isOpen()){
				if(log.isInfoEnabled())log.info("Frontend channel is closed.");
				readListener.closeAll();
				return;
			}
			try {
				if(!readListener.future.get().getSourceChannel().isOpen()){
					if(log.isInfoEnabled())log.info("Backend channel is closed.");
					readListener.closeAll();
					return;
				}
				int res=0;

				final ByteBuffer buffer = pooledBuffer.getResource();
				buffer.clear();

				while((res = readListener.future.get().getSourceChannel().read(buffer)) > 0){
					buffer.flip();
					long count = channel.write(buffer);
					boolean flushed = channel.flush();
					if(log.isInfoEnabled())log.info("Wrote "+res+" bytes from backend to client (flushed: "+flushed+")");
					readListener.totalWrites += count;
					backendToClientBytes.addAndGet(count);
					if(count != res){
						System.out.println("count != res -> "+count+" "+res);
					}
				}

				if(res == -1){
					if(log.isInfoEnabled())log.info("Backend End of stream.");
					readListener.closeAll();
					return;
				}
			} catch (IOException e) {
				e.printStackTrace();
				readListener.closeAll();
				return;
			}finally{
				if(log.isInfoEnabled())log.info("Suspending writes on front-end (sink)");
				if(log.isInfoEnabled())MDC.remove("channel");;
				channel.suspendWrites();
			}
			
		}
	}
	
	public static class Options {
		@Option(name = "-router_port", usage="port")
		public Integer router_port = 7080;
		
		@Option(name = "-backend_host", usage="host")
		public String backend_host = "192.168.1.133";
		
		@Option(name = "-backend_port", usage="port")
		public Integer backend_port = 8983;

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Options: router_port=").append(router_port).append(", backend_host=").append(backend_host)
					.append(", backend_port=").append(backend_port).append("");
			return builder.toString();
		}
	}

}