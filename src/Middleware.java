import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class Middleware{
	
//	private AsyncClient asyncClient;
//	private ThreadPoolExecutor executorPool;
//	private ArrayBlockingQueue<ClientRequestHandler> setQueue;
//	private ClientRequestHandler currentRequest;

	
	// locally specified variables
	private int setQueueSize = 10; // queue size for set
	private int getQueueSize = 30; // queue size for get
	private int numVirtualNodes = 200; // number of virtual nodes for servers (uniform key space distribution)

	// user specified variables at runtime
	private List<String> mcAddresses; // memcached server addresses
	private int numThreadsPTP; // maximum number of threads in Thread pool
	private int numReplication; // number of replication for set
	
	private ConcurrentHashMap<String, ManageQueue> delegateToQueue; // hashmap of <server IP, manageQueue> for storing server specific queue

	private LinkedList<Integer> keySpaceDistr = new LinkedList<Integer>(); // counter for key space distribution of hashing
	private ConsistentHash consistentHash; 

	/**
	 * TODO: add parameters
	 * @throws IOException 
	 * @throws NoSuchAlgorithmException 
	 */
	public Middleware(List<String> mcAddresses, int numThreadsPTP, int writeToCount) throws IOException{
		
		
		this.mcAddresses = mcAddresses;
		this.numThreadsPTP = numThreadsPTP;
		this.numReplication = writeToCount;
		
		for(int i=0; i<mcAddresses.size();i++){
			keySpaceDistr.add(0);
		}
				
		// create hash key for servers
		// replicate virtual nodes for uniform distribution of key space
		// TODO: explore uniform distribution of key space more 
		// look into if current strategy works
		this.consistentHash = new ConsistentHash(this.numVirtualNodes, this.mcAddresses);

		// create thread factory for threadPoolExecutor
		ThreadFactory threadFactory = Executors.defaultThreadFactory();
		// create rejection handler for threadPoolExecutor
		RejectedExecutionHandler rejectExecution = setupRejectionHandler();
		// create an internal structure of middleware
		setupInternalStructure(rejectExecution, threadFactory);
		
	}
	
	/**
	 * create rejection handlers for ThreadPoolExecutor
	 * rejectExecution - discarding policy
	 * customReject - custom rejection handler waiting for queue to be available (blocking)
	 * on default using rejectExecution
	 */
	private RejectedExecutionHandler setupRejectionHandler(){
		
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
        
		return rejectExecution;
	}
	
	/**
	 * initialize queue for set and ThreadPoolExecutor for get
	 * for each server; 
	 * save the queue in hashMap based on the server IP:Port key
	 * @throws IOException 
	 * 
	 */
	private void setupInternalStructure(RejectedExecutionHandler rejectExecution, ThreadFactory threadFactory) throws IOException{
		
		this.delegateToQueue = new ConcurrentHashMap<String, ManageQueue>();
		String curr_server;
		int curr_port;
		// create separate set queue and get thread pool for each memcachedServer
		for(int i=0; i<this.mcAddresses.size(); i++){

			curr_server = this.mcAddresses.get(i).split(":")[0];
			curr_port = Integer.parseInt(this.mcAddresses.get(i).split(":")[1]);
			
			// creating the ThreadPoolExecutor
			// when queue is full; right now when queue is full new task is disregarded 
			ThreadPoolExecutor currExecutor = new ThreadPoolExecutor(numThreadsPTP, 
														numThreadsPTP, 
														100, 
														TimeUnit.MILLISECONDS, 
														new ArrayBlockingQueue<Runnable>(getQueueSize), 
														threadFactory, 
														rejectExecution);
			
			ManageQueue tempQueue = new ManageQueue(this.setQueueSize, currExecutor);
			this.delegateToQueue.put(this.mcAddresses.get(i), tempQueue);
			AsyncClient tempClient = new AsyncClient(curr_server, curr_port, tempQueue.setQueue);
			new Thread(tempClient, this.mcAddresses.get(i)).start();
		}
	}
	
	
	/**
	 * monitor key space distribution between 
	 * memcached servers
	 * @param selectedServer - address of memcached server (IP:PORT)
	 */
	private void monitorKeySpace(String selectedServer){
		System.out.println("!!!!!: ");
		for(int i=0; i<this.mcAddresses.size(); i++){
			if(selectedServer.equals(this.mcAddresses.get(i))){
				keySpaceDistr.set(i, keySpaceDistr.get(i) + 1);
			}
			System.out.println(keySpaceDistr.get(i) + " ");
		}
	}
	
	
	/**
	 * process request from memaslap. 
	 * parse the request data: based on get/set command.
	 * hash the key, find the relevant memcached server
	 * and add the request to relevant queue based on 
	 * server identity and command (set/get) 
	 * 
	 * @param server - instance of Server class
	 * @param socket - socket channel from which request was received (memaslap)
	 * @param input - request data
	 * @throws Exception
	 */
	public void processRequest(Server server, SocketChannel socket, byte[] input) throws Exception{

		// initial parsing for set/get
		
		ClientRequestHandler clientRequestForward = new ClientRequestHandler(server, socket, ByteBuffer.wrap(input));
		String[] inputStr = new String(input, "UTF-8").split(" ");
//		System.out.println("received: " + new String(input, "UTF-8"));
		
		if(inputStr.length >= 2){

			// get the memcached server address to which the request key belongs
			String selectedServer = this.consistentHash.get(inputStr[1].trim());
			
			// monitor the key space distribution of 
//			monitorKeySpace(selectedServer);
			
			// add request to relevant queue
			if(inputStr[0].equals("get")){
//				this.executorPool.execute(new SyncClient(clientRequestForward, selectedServer));	
				this.delegateToQueue.get(selectedServer).executor.execute(new SyncClient(clientRequestForward, selectedServer));
			}
			else if(inputStr[0].equals("set")){
				// block if setQueue is full
//				this.setQueue.offer(clientRequestForward);
				this.delegateToQueue.get(selectedServer).setQueue.offer(clientRequestForward);
//				System.out.println(this.setQueue.size());
//				this.asyncClient.sendToMemCache(clientRequestForward);

			}
		}
		
	}
	
//	public void run(){
				
//		while(true){
//			
//			
//			// iterate over get/set queues for each memcached server
//			Iterator<ManageQueue> queueIterator = this.delegateToQueue.values().iterator();
//			while(queueIterator.hasNext()){
//				ManageQueue currQueue = queueIterator.next();
//				if((currentRequest = currQueue.setQueue.poll()) != null){
//					try {
//						currQueue.asyncClient.sendToMemCache(currentRequest);
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
//				
//			}
			
			
//		if((currentRequest = this.setQueue.poll()) != null){
//			try {
//				this.asyncClient.sendToMemCache(currentRequest);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		
//		}
		
//	}
	
	
}
