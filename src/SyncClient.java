import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;

public class SyncClient implements Runnable{
	
	private SocketChannel socketChannel;
	private RequestData clientHandler;
	private String memcachedServer;
	private int memcachedPort;
	private ArrayBlockingQueue<RequestData> getQueue;
	private ByteBuffer readBuff = ByteBuffer.allocate(2024);
	private byte[] buff;
	
	/**
	 * constructor
	 * @throws IOException 
	 */
	public SyncClient(ArrayBlockingQueue<RequestData> getQueue, String selectedServer) throws IOException{
		this.getQueue = getQueue;
		this.memcachedServer = selectedServer.split(":")[0];
		this.memcachedPort = Integer.parseInt(selectedServer.split(":")[1]);
		initConnection();		
	}
	
	/**
	 * initiate connection to memcahed server
	 * blocking mode - synchronous
	 * @throws IOException
	 */
	private void initConnection() throws IOException{
		this.socketChannel = SocketChannel.open();
		this.socketChannel.configureBlocking(true);
		this.socketChannel.connect(new InetSocketAddress(this.memcachedServer, this.memcachedPort));
	}
	
	public void run(){
		
		try{
		
			while(true){
				// take from the associated GET queue
				// blocks the thread if the queue is empty 
				clientHandler = this.getQueue.take();
				
				// calculate time requets spend in queue
				clientHandler.calculate_T_queue(); // logging data
				
				readBuff.clear();
				// set time request was sent to memcached server
				clientHandler.set_time_server_send(); // logging data
				
				// write to memcached server
			    this.socketChannel.write(clientHandler.data);
				// read from memcached server
				int bytesRead = this.socketChannel.read(readBuff);
				if(bytesRead > 0){
					// calculate time request spent time in server
					clientHandler.calculate_T_server(); // logging data
					buff = new byte[readBuff.position()];
					readBuff.flip();
					readBuff.get(buff);
					
					// logging data
					// set success flag to false if failed
					if(new String(buff).trim().toLowerCase().equals("end")){
						clientHandler.set_success_flag(false);
					}
					// set the response in RequestData instance
					clientHandler.setResponse(ByteBuffer.wrap(buff));
					// send back to memaslap using Server class method
					clientHandler.server.send(clientHandler);				
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
