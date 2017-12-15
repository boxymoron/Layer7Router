import java.nio.ByteBuffer;
import java.util.function.Function;

public class ByteBufferTest {

	public static void main(String[] args){


		time((a)->{
			ByteBuffer dbuffer = ByteBuffer.allocateDirect(1024*32);
			for(int j=0;j<1024*1024;j++){
				for(int i=0;i<1024*16;i++){
					dbuffer.putChar('f');
				}

				dbuffer.flip();

				StringBuilder sb = new StringBuilder();
				while(dbuffer.hasRemaining()){

					sb.append(dbuffer.getChar());
				}
				//System.out.println(dbuffer);

				dbuffer.clear();

			}
			return a;
		});

		time((a)->{
			ByteBuffer buff = ByteBuffer.allocate(1024*32);
			for(int j=0;j<1024*1024;j++){
				for(int i=0;i<1024*16;i++){
					buff.putChar('f');
				}

				buff.flip();

				StringBuilder sb = new StringBuilder();
				while(buff.hasRemaining()){

					sb.append(buff.getChar());
				}
				//System.out.println(dbuffer);

				buff.clear();

			}
			return a;
		});
	}

	public static void time(Function f){
		long start = System.currentTimeMillis();
		f.apply(null);
		System.out.println("Took "+(System.currentTimeMillis() - start)+" ms");
	}
}
