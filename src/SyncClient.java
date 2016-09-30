import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;

public class SyncClient implements Runnable{
	
	private SocketChannel socketChannel;
	private ClientRequestHandler clientHandler;
	
	/**
	 * constructor
	 * @throws IOException 
	 */
	public SyncClient(ClientRequestHandler clientHandler) throws IOException{
		this.clientHandler = clientHandler;
		

	}
	
	public void run(){
		
		try{
		
			// initiate connection
//			System.out.println(Thread.currentThread().getName());

			this.socketChannel = SocketChannel.open();
			this.socketChannel.configureBlocking(true);
			this.socketChannel.connect(new InetSocketAddress("127.0.0.1", 8000));
			ByteBuffer readBuff = ByteBuffer.allocate(4024);
			// write to memcached server
			while(clientHandler.data.hasRemaining()) {
			    this.socketChannel.write(clientHandler.data);
			}
			// read from memcached server
			int bytesRead = this.socketChannel.read(readBuff);
			if(bytesRead > 0){
				clientHandler.server.send(clientHandler.socket, readBuff.array());
			}
			
			
			
		}
		catch(IOException e){
			e.printStackTrace();
		}
		finally{
			try {
				this.socketChannel.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	
}
