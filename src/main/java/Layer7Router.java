import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CancellationException;

import org.jboss.logging.Logger;
import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.OptionMap;
import org.xnio.Options;
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
			.set(Options.WORKER_IO_THREADS, 4)
			.getMap();
	static XnioWorker worker;
	static XnioWorker worker2;

	public Layer7Router() throws Exception{

	}

	public static void main(String[] args) throws Exception { 
		worker = xnio.createWorker(options);
		worker2 = xnio.createWorker(options);

		final ChannelListener<AcceptingChannel<StreamConnection>> acceptListener2 = new ChannelListener<AcceptingChannel<StreamConnection>>() {
			@Override
			public final void handleEvent(AcceptingChannel<StreamConnection> channel) {
				try {
					StreamConnection accepted;
					while ((accepted = channel.accept()) != null) {
						log.info("Accepted: " + accepted.getPeerAddress());

						final MyReadListener readListener = new MyReadListener();
						accepted.getSourceChannel().setReadListener(readListener);
						accepted.getSourceChannel().setCloseListener(readListener);

						final MyWriteListener writeListener = new MyWriteListener(readListener);
						accepted.getSinkChannel().setWriteListener(writeListener);
						accepted.getSinkChannel().setCloseListener(writeListener);

						accepted.getSourceChannel().resumeReads();
						accepted.getSinkChannel().resumeWrites();
					}
				} catch (IOException e) {
					e.printStackTrace();
					try {
						channel.close();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
				}
			}
		};

		final AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(new InetSocketAddress(8080), acceptListener2, OptionMap.EMPTY);
		server.resumeAccepts();

		log.info("Listening on " + server.getLocalAddress());

	}

	public final static class MyReadListener implements ChannelListener<ConduitStreamSourceChannel> {
		final static Logger log = Logger.getLogger(MyReadListener.class);
		final public ByteBuffer headersBuffer = ByteBuffer.allocate(1024*32);
		final public ByteBuffer buffer = ByteBuffer.allocateDirect(1024*32);
		final public ByteBuffer readBuffer = ByteBuffer.allocateDirect(1024*32);
		public volatile boolean initHeaders=true;
		public volatile boolean init=true;

		IoFuture<StreamConnection> future;
		@Override
		public void handleEvent(ConduitStreamSourceChannel channel) {
			try {
				if(!channel.isOpen()){
					channel.close();
					return;
				}
				log.info(channel);
				log.info("channel open? "+channel.isOpen());
				long res;
				long res2=0;
				int total=0;
				int totalWritten=0;
				buffer.clear();
				while ((res = channel.read(buffer)) > 0) {
					buffer.flip();
					log.info(buffer.toString());
					if(initHeaders){
						log.info("Reading headers...");

						String header = StandardCharsets.UTF_8.decode(buffer).toString();
						buffer.rewind();
						String[] lines = header.split("\n|\r\n");

						log.info("Read "+res+" bytes");
						total+=res;
						log.info(lines[0]);
						buffer.get(headersBuffer.array(),0, buffer.remaining());
						buffer.rewind();
						headersBuffer.limit(buffer.remaining());
						//System.out.println(new String(headersBuffer.array()));

						future = worker2.openStreamConnection(new InetSocketAddress("192.168.1.150", 80), null, options);
						future.await();
						initHeaders=false;

						log.info(headersBuffer.toString());
						while((res2 = future.get().getSinkChannel().write(headersBuffer)) > 0l){
							log.info("Wrote "+res2+" header bytes");
							totalWritten += res2;
						}
						future.get().getSinkChannel().flush();
						totalWritten+=res2;
						init=false;
					}else{

						buffer.get(headersBuffer.array(),0, buffer.remaining());
						buffer.rewind();
						headersBuffer.limit(buffer.remaining());

						//System.out.println(new String(headersBuffer.array()));

						total+=buffer.remaining();
						while((res2 = future.get().getSinkChannel().write(buffer)) > 0l){
							future.get().getSinkChannel().flush();
							log.info("Wrote "+res2+" non-header bytes");
							totalWritten += res2;
						}
					}
				}
				log.info("Read  total of: "+total+" bytes");
				log.info("Write total of: "+totalWritten+" bytes");
				if(res == -1){
					log.info("End of stream. Closing channel.");
					channel.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
				try {
					channel.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				try {
					if(future != null){
						future.get().close();
					}
				} catch (CancellationException | IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

	public final static class MyWriteListener implements ChannelListener<ConduitStreamSinkChannel> {
		final ByteBuffer buffer = ByteBuffer.allocateDirect(1024*32);
		MyReadListener readListener;

		public MyWriteListener(MyReadListener readListener){
			this.readListener = readListener;
		}
		@Override
		public void handleEvent(ConduitStreamSinkChannel channel) {
			if(readListener.initHeaders){
				return;
			}
			try {
				//System.out.println("MyWriteListener: "+channel);
				int res=0;
				int total=0;
				buffer.clear();
				while((res = readListener.future.get().getSourceChannel().read(buffer)) > 0){
					buffer.flip();
					long count = channel.write(buffer);
					channel.flush();
					total += count;
				}
				if(res == -1){
					try{
						channel.close();
					}catch(IOException e){
						e.printStackTrace();
					}
					if(readListener.future != null){
						try{
							readListener.future.get().getSourceChannel().close();
						}catch(IOException e){
							e.printStackTrace();
						}
						try{
							readListener.future.get().getSinkChannel().close();
						}catch(IOException e){
							e.printStackTrace();
						}
					}
				}
				//log.info("Write total of: "+total+" bytes");
			} catch (IOException e) {
				e.printStackTrace();
				try {
					channel.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				if(readListener.future != null){
					try{
						readListener.future.get().close();
					}catch(IOException e1){
						e.printStackTrace();
					}
				}
			}
		}
	}

}