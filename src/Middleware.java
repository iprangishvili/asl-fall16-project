import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.sun.security.ntlm.Client;
import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;


public class Middleware implements Runnable{
	
	private AsyncClient asyncClient;
	private ThreadPoolExecutor executorPool;
	private ArrayBlockingQueue<ClientRequestHandler> setQueue;
	private int maxThreadSize;
	private int queueSize;
	private LinkedList<String> serverList;
	
	private ConsistentHash consistentHash;

	/**
	 * TODO: add parameters
	 * @throws IOException 
	 */
	public Middleware() throws IOException{
		
		// populate list with servers TODO: put it in arguments
		serverList = new LinkedList<String>();
		serverList.add("127.0.0.1:8000");
		serverList.add("127.0.0.1:7070");
		serverList.add("127.0.0.1:5050");
		
		// create cache for servers
		
		try {
			this.consistentHash = new ConsistentHash(200, serverList);
		} catch (NoSuchAlgorithmException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		

		this.maxThreadSize = 50;
		this.queueSize = 40;
		
		// initialize set/get command queue
		this.setQueue = new ArrayBlockingQueue<ClientRequestHandler>(10);
		
		//Get the ThreadFactory implementation to use
		ThreadFactory threadFactory = Executors.defaultThreadFactory();
		
		RejectedExecutionHandler rejectExecution = new ThreadPoolExecutor.DiscardPolicy();
		
		// block if queue is full
		RejectedExecutionHandler customReject = new RejectedExecutionHandler() {
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                try {
                	System.out.println("waiting for queue to be available");
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted while submitting task", e);
                }
            }
            };
	 	
        // creating the ThreadPoolExecutor
		// when queue is full; right now when queue is full new task is disregarded 
		this.executorPool = new ThreadPoolExecutor(maxThreadSize, 
													maxThreadSize, 
													0L, 
													TimeUnit.MILLISECONDS, 
													new ArrayBlockingQueue<Runnable>(queueSize), 
													threadFactory, 
													rejectExecution);
	
        
		// initiate client connection to memcached server
		this.asyncClient = new AsyncClient();
		new Thread(asyncClient).start();
		

	}
	
	public void processRequest(Server server, SocketChannel socket, byte[] input, int count) throws Exception{
		//TODO: process hash key; consistent hashing;
		// initial parsing for set/get
		
		ClientRequestHandler clientRequestForward = new ClientRequestHandler(server, socket, ByteBuffer.wrap(input), count);
		String[] inputStr = new String(input).split(" ");
		
		if(inputStr.length >= 2){
			String selectedServer = this.consistentHash.get(inputStr[1]);
			System.out.println("selected server: " + selectedServer);
			if(inputStr[0].equals("get")){
				this.executorPool.execute(new SyncClient(clientRequestForward, selectedServer));				
			}
			else if(inputStr[0].equals("set")){
				
				// block if setQueue is full
				clientRequestForward.memCachedSetting(selectedServer);
				this.setQueue.offer(clientRequestForward);
//				System.out.println(this.setQueue.size());
//				this.asyncClient.sendToMemCache(clientRequestForward);

			}
		}
		
	}
	
	public void run(){
		
		ClientRequestHandler currentRequest;
		
		while(true){
			
			if((currentRequest = this.setQueue.poll()) != null){
				try {
					this.asyncClient.sendToMemCache(currentRequest);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		// TODO: thread pooling; waiting queue; mitigate sendToMemcache here
		
	}
	
	
}
