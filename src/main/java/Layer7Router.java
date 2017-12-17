import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.logging.Logger;
import org.xnio.ByteBufferSlicePool;
import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Pooled;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;

public final class Layer7Router {

	final static Logger log = Logger.getLogger(Layer7Router.class);

	final static Xnio xnio = Xnio.getInstance();
	final static OptionMap options = OptionMap.builder()
			.set(Options.ALLOW_BLOCKING, false)
			.set(Options.RECEIVE_BUFFER, 1024*32)
			.set(Options.SEND_BUFFER, 1024*32)
			.set(Options.READ_TIMEOUT, 30000)
			.set(Options.WRITE_TIMEOUT, 30000)
			.set(Options.MAX_INBOUND_MESSAGE_SIZE, 1024 * 1024 * 1024 * 2)
			.set(Options.MAX_OUTBOUND_MESSAGE_SIZE, 1024 * 1024 * 1024 * 2)
			.set(Options.USE_DIRECT_BUFFERS, true)
			.set(Options.WORKER_IO_THREADS, 2)
			.getMap();
	static XnioWorker worker;
	
	final static AtomicInteger totalAccepted = new AtomicInteger();
	final static AtomicInteger listenersCount = new AtomicInteger();
	final static AtomicInteger writersCount = new AtomicInteger();	
	final static AtomicLong clientToBackendBytes = new AtomicLong();
	final static AtomicLong backendToClientBytes = new AtomicLong();

	static ByteBufferSlicePool pool = new ByteBufferSlicePool(1024*32, 1024*32*1024);//allocate 1024 buffers of 32KB each, for 32MB worth of buffers

	public static void main(String[] args) throws Exception {
		worker = xnio.createWorker(options);

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

		final AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(new InetSocketAddress(8080), acceptListener2, OptionMap.EMPTY);
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
		
		@Override
		public final void handleEvent(final ConduitStreamSourceChannel channel) {
			if(!streamConnection.isOpen()|| !channel.isOpen()){	
				closeAll();
				return;
			}
			try {
				int totalRead=0;
				int totalWritten=0;

				final ByteBuffer buffer = pooledBuffer.getResource();
				
				if(!initHeaders && writeHeaders){
					totalWritten = writeHeaders(buffer);
				}

				long clientReadBytes=0;
				while ((clientReadBytes = channel.read(buffer)) > 0) {
					totalRead+=clientReadBytes;
					buffer.flip();
					if(log.isInfoEnabled())log.info(streamConnection.toString()+" "+buffer.toString());
					if(initHeaders){
						initHeaders(channel, buffer);
						return;
					}else{
						totalWritten = writeBody(buffer);
					}
				}

				if(clientReadBytes == -1){
					if(log.isInfoEnabled())log.info(streamConnection.toString()+" End of stream. Closing channel: "+channel);
					closeAll();
				}
				if(log.isInfoEnabled())log.info(streamConnection.toString()+"Suspending reads on channel: "+channel);
				channel.suspendReads();
			} catch (IOException e) {
				e.printStackTrace();
				closeAll();
			}
		}

		private void initHeaders(final ConduitStreamSourceChannel channel, final ByteBuffer buffer) {
			if(log.isInfoEnabled())log.info(streamConnection.toString()+" Read "+buffer.limit()+" header bytes");
			if(log.isInfoEnabled())log.info(streamConnection.toString()+" Suspending writes on channel: "+streamConnection.getSinkChannel());
			streamConnection.getSinkChannel().suspendWrites();//start out suspended, as soon as the backend source channel is ready, we will get resumed
			if(log.isInfoEnabled())log.info(streamConnection.toString()+" Reading headers...");
			String[] lines = StandardCharsets.UTF_8.decode(buffer).toString().split("\r\n");
			//if(log.isInfoEnabled())log.info(lines[0]);
			for(String line: lines){
				//System.out.println(line);
			}
			buffer.rewind();
			
			if(log.isInfoEnabled())log.info(streamConnection.toString()+" Suspending reads on channel: "+channel);
			channel.suspendReads();
			future = worker.openStreamConnection(new InetSocketAddress("192.168.1.133", 8983), backendChannel -> {
					if(log.isInfoEnabled())log.info(streamConnection.toString()+" Resuming reads on channel: "+channel);
					channel.wakeupReads();
					backendChannel.setCloseListener(backendChannel2 -> {
						closeAll();
						streamConnection.getSinkChannel().resumeWrites();
					});
					backendChannel.getSourceChannel().getReadSetter().set(channel2 ->{
						//if(log.isInfoEnabled())log.info(streamConnection.toString()+"Resuming writes on channel: "+streamConnection.getSinkChannel());//TODO why is this called multiple times un succession
						streamConnection.getSinkChannel().resumeWrites();//resume writes to client
					});
					backendChannel.getSinkChannel().getWriteSetter().set(channel2 ->{
						//if(log.isInfoEnabled())log.info(streamConnection.toString()+"Resuming reads on channel: "+channel);
						channel.resumeReads();
					});
					backendChannel.getSourceChannel().resumeReads();
					backendChannel.getSinkChannel().resumeWrites();
			}, options);
			this.initHeaders=false;
			this.writeHeaders = true;
		}

		private int writeHeaders(final ByteBuffer buffer) throws IOException {
			int totalWritten=0;
			int backendWriteBytes=0;
			if(log.isInfoEnabled())log.info(streamConnection.toString()+" "+buffer.toString());
			//System.out.println(Buffers.createDumper(buffer, 1, 4).toString());
			final ConduitStreamSinkChannel sink = future.get().getSinkChannel();
			while((backendWriteBytes = sink.write(buffer)) > 0l){//write to backend
				sink.flush();
				if(log.isInfoEnabled())if(log.isInfoEnabled())log.info(streamConnection.toString()+" Wrote "+backendWriteBytes+" header bytes to backend");
				totalWritten += backendWriteBytes;
			}
			clientToBackendBytes.addAndGet(totalWritten);
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
				if(log.isInfoEnabled())log.info(streamConnection.toString()+" Wrote "+backendWriteBytes+" non-header bytes to backend (flushed: "+flushed+")");
				totalWritten += backendWriteBytes;
			}
			clientToBackendBytes.addAndGet(totalWritten);
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
			if(log.isInfoEnabled())log.info(streamConnection.toString()+" Closing all resources.");
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
			if(!readListener.streamConnection.isOpen() || !channel.isOpen()){
				readListener.closeAll();
				return;
			}
			try {
				if(!readListener.future.get().getSourceChannel().isOpen()){
					readListener.closeAll();
					return;
				}
				int res=0;
				int total=0;
				final ByteBuffer buffer = pooledBuffer.getResource();
				buffer.clear();

				while((res = readListener.future.get().getSourceChannel().read(buffer)) > 0){
					buffer.flip();
					long count = channel.write(buffer);
					channel.flush();
					total += count;
				}
				backendToClientBytes.addAndGet(total);
				if(log.isInfoEnabled())log.info(readListener.streamConnection.toString()+" Write from backend to client total of: "+total+" bytes");
				if(res == -1){
					readListener.closeAll();
					return;
				}
			} catch (IOException e) {
				e.printStackTrace();
				readListener.closeAll();
				return;
			}
			channel.suspendWrites();
		}
	}

}