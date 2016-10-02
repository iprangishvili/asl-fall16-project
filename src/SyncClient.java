import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;

public class SyncClient implements Runnable{
	
	private SocketChannel socketChannel;
	private ClientRequestHandler clientHandler;
	private String memcachedServer;
	private int memcachedPort;
	
	/**
	 * constructor
	 * @throws IOException 
	 */
	public SyncClient(ClientRequestHandler clientHandler, String selectedServer) throws IOException{
		this.clientHandler = clientHandler;
		this.memcachedServer = selectedServer.split(":")[0];
		this.memcachedPort = Integer.parseInt(selectedServer.split(":")[1]);
	}
	
	public void run(){
		
		try{
		
			// initiate connection
//			System.out.println(Thread.currentThread().getName());

			this.socketChannel = SocketChannel.open();
			this.socketChannel.configureBlocking(true);
			this.socketChannel.connect(new InetSocketAddress(this.memcachedServer, this.memcachedPort));
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
