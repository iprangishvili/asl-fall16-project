import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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
	private int setQueueSize = 1000; // queue size for set
	private int getQueueSize = 1000; // queue size for get
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
//		ThreadFactory threadFactory = Executors.defaultThreadFactory();
		// create rejection handler for threadPoolExecutor
//		RejectedExecutionHandler rejectExecution = setupRejectionHandler();
		
		
		// create an internal structure of middleware
		setupInternalStructure();
		
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
	private void setupInternalStructure() throws IOException{
		
		this.delegateToQueue = new ConcurrentHashMap<String, ManageQueue>();
		
		// create separate set queue and get thread pool for each memcachedServer
		for(int i=0; i<this.mcAddresses.size(); i++){
			
			// creating the ThreadPoolExecutor
			// when queue is full; right now when queue is full new task is disregarded 
//			ThreadPoolExecutor currExecutor = new ThreadPoolExecutor(numThreadsPTP, 
//														numThreadsPTP, 
//														99999, 
//														TimeUnit.SECONDS, 
//														new ArrayBlockingQueue<Runnable>(getQueueSize), 
//														threadFactory, 
//														rejectExecution);
//			
			ManageQueue tempQueue = new ManageQueue(this.setQueueSize, this.getQueueSize);
			
			// Sync client threads
			for(int j=0; j<this.numThreadsPTP; j++){
				new Thread(new SyncClient(tempQueue.getQueue, this.mcAddresses.get(i))).start();
			}
			
//			AsyncClient tempClient = new AsyncClient(curr_server, curr_port, tempQueue.setQueue);
			new Thread(new AsyncClient(this.mcAddresses, i, this.numReplication ,tempQueue.setQueue), this.mcAddresses.get(i)).start();
			this.delegateToQueue.put(this.mcAddresses.get(i), tempQueue);

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

	
			
			// monitor the key space distribution of 
//			monitorKeySpace(selectedServer);
			
			// add request to relevant queue
			if(inputStr[0].equals("get")){
				// get the memcached server address to which the request key belongs		
				String selectedServer = this.consistentHash.get(inputStr[1].trim());
//				this.executorPool.execute(new SyncClient(clientRequestForward, selectedServer));	
//				this.delegateToQueue.get(selectedServer).executor.execute(new SyncClient(clientRequestForward, selectedServer));
				this.delegateToQueue.get(selectedServer).getQueue.put(clientRequestForward);
			}
			else if(inputStr[0].equals("set")){				
				// check for replication
				if(this.numReplication == 1){
					
					String selectedServer = this.consistentHash.get(inputStr[1].trim());
					this.delegateToQueue.get(selectedServer).setQueue.put(clientRequestForward);

				}
				else if(this.numReplication > 1){
//					System.out.println("here");
					ArrayList<String> selectedServers = new ArrayList<String>(this.consistentHash.getWithReplica(inputStr[1].trim(), this.numReplication));
//					Iterator<String> selectedServer.iterator();
					clientRequestForward.setReplicaAddress(selectedServers);
//					System.out.println("replication to server: " + curr_server);
					this.delegateToQueue.get(selectedServers.get(0)).setQueue.put(clientRequestForward);
				}

				
				// block if setQueue is full
//				this.setQueue.offer(clientRequestForward);
//				this.asyncClient.sendToMemCache(clientRequestForward);

			}
		}
		
	}
	
}
