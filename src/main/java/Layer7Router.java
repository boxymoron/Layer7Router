import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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
			.getMap();
	static XnioWorker worker;
	static XnioWorker worker2;

	public Layer7Router() throws Exception{

	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception { 
		worker = xnio.createWorker(options);
		worker2 = xnio.createWorker(options);
		final Charset charset = Charset.forName("utf-8");

		/*
        // First define the listener that actually is run on each connection.
        final ChannelListener<ConnectedStreamChannel> readListener = new ChannelListener<ConnectedStreamChannel>() {
        	final ByteBuffer buffer = ByteBuffer.allocateDirect(1024*32);
            public void handleEvent(ConnectedStreamChannel incomingCh) {
                int res;
                try {
                    System.out.println("Incoming from: "+incomingCh.getPeerAddress().toString());
                    while ((res = incomingCh.read(buffer)) > 0) {
                    	System.out.println(buffer.toString());
                        buffer.flip();
                        String header = StandardCharsets.UTF_8.decode(buffer).toString();
        				String[] lines = header.split("\n");
        				for(String line : lines){
        					System.out.println(line);
        				}
        				buffer.rewind();

                    }

                    final IoFuture<ConnectedStreamChannel> futureConnection = worker2.connectStream(new InetSocketAddress("blogs.boxymoron.com", 80), null, OptionMap.EMPTY);
                    final ConnectedStreamChannel outboundCh = futureConnection.get(); // throws exceptions
    				Channels.writeBlocking(outboundCh, buffer);
    				Channels.flushBlocking(outboundCh);

                    ByteBuffer recvBuf = ByteBuffer.allocate(1024*32);
                    while (Channels.readBlocking(outboundCh, recvBuf) != -1) {
                        recvBuf.flip();
                        //final CharBuffer chars = charset.decode(recvBuf);
                        //System.out.print(chars);
                        //recvBuf.rewind();
                        incomingCh.write(recvBuf);
                    }

                    if (res == -1) {
                        incomingCh.close();
                        outboundCh.close();
                        buffer.clear();
                        recvBuf.clear();
                    } else {
                        incomingCh.resumeReads();
                        outboundCh.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    IoUtils.safeClose(incomingCh);
                }
            }
        };

        // Create an accept listener.
        final ChannelListener<AcceptingChannel<ConnectedStreamChannel>> acceptListener = new ChannelListener<AcceptingChannel<ConnectedStreamChannel>>() {
            public void handleEvent(final AcceptingChannel<ConnectedStreamChannel> channel) {
                try {
                    ConnectedStreamChannel accepted;
                    // channel is ready to accept zero or more connections
                    while ((accepted = channel.accept()) != null) {
                        System.out.println("accepted " + accepted.getPeerAddress());
                        // stream channel has been accepted at this stage.
                        accepted.getReadSetter().set(readListener);
                        // read listener is set; start it up
                        accepted.resumeReads();
                    }
                } catch (IOException ignored) {
                }
            }
        };*/

		//AcceptingChannel<? extends ConnectedStreamChannel> server = worker.createStreamServer(new InetSocketAddress(8080), acceptListener, OptionMap.EMPTY);
		// lets start accepting connections
		//server.resumeAccepts();

		final ChannelListener<AcceptingChannel<StreamConnection>> acceptListener2 = new ChannelListener<AcceptingChannel<StreamConnection>>() {
			@Override
			public final void handleEvent(AcceptingChannel<StreamConnection> channel) {
				try {
					StreamConnection accepted;
					// channel is ready to accept zero or more connections
					while ((accepted = channel.accept()) != null) {
						System.out.println("accepted " + accepted.getPeerAddress());
						// stream channel has been accepted at this stage.
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
				}
			}
		};

		final AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(new InetSocketAddress(8080), acceptListener2, OptionMap.EMPTY);
		server.resumeAccepts();

		System.out.println("Listening on " + server.getLocalAddress());

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
						System.out.println(new String(headersBuffer.array()));

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

						System.out.println(new String(headersBuffer.array()));

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
			} catch (Exception e) {
				e.printStackTrace();
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
					readListener.future.get().getSourceChannel().close();
					readListener.future.get().getSinkChannel().close();
				}
				//log.info("Write total of: "+total+" bytes");
			} catch (IOException e) {
				e.printStackTrace();
				try {
					channel.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

}