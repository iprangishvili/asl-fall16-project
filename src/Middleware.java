import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.sun.security.ntlm.Client;


public class Middleware implements Runnable{
	
	private AsyncClient asyncClient;
	private ThreadPoolExecutor executorPool;

	/**
	 * TODO: add parameters
	 * @throws IOException 
	 */
	public Middleware() throws IOException{

		int maxThreadSize = 50;
		int queueSize = 20;
		
		//Get the ThreadFactory implementation to use
		ThreadFactory threadFactory = Executors.defaultThreadFactory();
		RejectedExecutionHandler rejectExecution = new ThreadPoolExecutor.DiscardPolicy();
		
        //creating the ThreadPoolExecutor
        this.executorPool = new ThreadPoolExecutor(maxThreadSize, maxThreadSize, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(queueSize), threadFactory, rejectExecution);
        //start the monitoring thread

		
		// initiate client connection to memcached server
		this.asyncClient = new AsyncClient();
		new Thread(asyncClient).start();
	}
	
	public void processRequest(Server server, SocketChannel socket, byte[] input, int count) throws Exception{
		//TODO: process hash key; consistent hashing;
		// initial parsing for set/get
		
		ClientRequestHandler clientRequestForward = new ClientRequestHandler(server, socket, ByteBuffer.wrap(input), count);
		String[] inputStr = new String(input).split(" ");
		if(inputStr.length > 0){
			if(inputStr[0].equals("get")){
//				System.out.println("GET " + inputStr[1]);
				this.executorPool.execute(new SyncClient(clientRequestForward));
//				System.out.println("is get blocked ?");
			}
			else if(inputStr[0].equals("set")){
//				System.out.println("set command: " + inputStr[1]);
				asyncClient.sendToMemCache(clientRequestForward);
//				System.out.println("is set blocked ?");

			}
		}
		
	}
	
	public void run(){

		// TODO: thread pooling; waiting queue; mitigate sendToMemcache here
		
	}
	
	
}
