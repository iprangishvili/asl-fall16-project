import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

public class SyncClient implements Runnable{
	
	private SocketChannel socketChannel;
	private RequestData clientHandler;
	private String memcachedServer;
	private int memcachedPort;
	private ArrayBlockingQueue<RequestData> getQueue;
	private ByteBuffer readBuff = ByteBuffer.allocate(2024);
	private byte[] buff;

    private Logger LOGGER;
	
	/**
	 * constructor
	 * @throws IOException 
	 */
	public SyncClient(ArrayBlockingQueue<RequestData> getQueue, String selectedServer, Logger LOGGER) throws IOException{
		this.getQueue = getQueue;
		this.memcachedServer = selectedServer.split(":")[0];
		this.memcachedPort = Integer.parseInt(selectedServer.split(":")[1]);
		initConnection();
		
		this.LOGGER = LOGGER;
	}
	
	private void initConnection() throws IOException{
		this.socketChannel = SocketChannel.open();
		this.socketChannel.configureBlocking(true);
		this.socketChannel.connect(new InetSocketAddress(this.memcachedServer, this.memcachedPort));
	}
	
	private void writeToLog(RequestData rdata){
//		String logMsg = String.format("%s%r, %d%n%r, %d%n%r, %d%n%r, %s%r", "GET", rdata.get_T_mw(), rdata.get_T_queue(), rdata.get_T_server(), rdata.get_success_flag());
		String logMsg = "GET," + rdata.get_T_mw() + "," + rdata.get_T_queue() + "," + rdata.get_T_server() + "," + rdata.get_success_flag();
		LOGGER.info(logMsg);
	}
	
	
	public void run(){
		
		try{
		
			while(true){
				clientHandler = this.getQueue.take();
				clientHandler.calculate_T_queue(); // logging data
				readBuff.clear();
				clientHandler.set_time_server_send(); // logging data
				// write to memcached server
			    this.socketChannel.write(clientHandler.data);
				// read from memcached server
				int bytesRead = this.socketChannel.read(readBuff);
			    clientHandler.calculate_T_server(); // logging data
				buff = new byte[readBuff.position()];
				readBuff.flip();
				readBuff.get(buff);
				if(bytesRead > 0){
					// logging
					if(!new String(buff).toLowerCase().equals("error")){
						clientHandler.set_success_flag(true);
					}
					else{
						clientHandler.set_success_flag(false);
					}
					clientHandler.calculate_T_mw();
					
					writeToLog(clientHandler);
					
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
