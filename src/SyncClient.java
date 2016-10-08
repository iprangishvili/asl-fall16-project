import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

public class SyncClient implements Runnable{
	
	private SocketChannel socketChannel;
	private ClientRequestHandler clientHandler;
	private String memcachedServer;
	private int memcachedPort;
	private ArrayBlockingQueue<ClientRequestHandler> getQueue;
	private ByteBuffer readBuff = ByteBuffer.allocate(2024);
	private byte[] buff;

	
	/**
	 * constructor
	 * @throws IOException 
	 */
	public SyncClient(ArrayBlockingQueue<ClientRequestHandler> getQueue, String selectedServer) throws IOException{
		this.getQueue = getQueue;
		this.memcachedServer = selectedServer.split(":")[0];
		this.memcachedPort = Integer.parseInt(selectedServer.split(":")[1]);
		initConnection();
	}
	
	private void initConnection() throws IOException{
		this.socketChannel = SocketChannel.open();
		this.socketChannel.configureBlocking(true);
		this.socketChannel.connect(new InetSocketAddress(this.memcachedServer, this.memcachedPort));
	}
	
	
	public void run(){
		
		try{
		
			while(true){
				clientHandler = this.getQueue.take();
				readBuff.clear();
				// write to memcached server
			    this.socketChannel.write(clientHandler.data);
				// read from memcached server
				int bytesRead = this.socketChannel.read(readBuff);
				buff = new byte[readBuff.position()];
				readBuff.flip();
				readBuff.get(buff);
				if(bytesRead > 0){
//					System.out.println("GET response: " + new String(buff));
					clientHandler.server.send(clientHandler.socket, buff);
				
				}
			}
		}
		catch(IOException e){
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
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
