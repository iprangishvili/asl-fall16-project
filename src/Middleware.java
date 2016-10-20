import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;



public class Middleware{
		
	// locally specified variables
	private int setQueueSize = 10000; // queue size for set
	private int getQueueSize = 10000; // queue size for get
	private int numVirtualNodes = 200; // number of virtual nodes for servers (uniform key space distribution)

	// user specified variables at runtime
	private List<String> mcAddresses; // memcached server addresses
	private int numThreadsPTP; // maximum number of threads in Thread pool
	private int numReplication; // number of replication for set
	
	private ConcurrentHashMap<String, ManageQueue> delegateToQueue; // hashmap of <server IP, manageQueue> for storing server specific queue
	private ConsistentHash consistentHash; 

	/**
	 * @throws IOException 
	 * @throws NoSuchAlgorithmException 
	 */
	public Middleware(List<String> mcAddresses, int numThreadsPTP, int writeToCount) throws IOException{
				
		this.mcAddresses = mcAddresses;
		this.numThreadsPTP = numThreadsPTP;
		this.numReplication = writeToCount;
		
		// create an instance of consistent hash class
		// adds hash values of the memcached servers on the circle
		this.consistentHash = new ConsistentHash(this.numVirtualNodes, this.mcAddresses);
		
		// create an internal structure of middleware
		setupInternalStructure();
		
	}
	
	/**
	 * initialize ManageQueue instance holding set/get queue and asynchronous client
	 * instance, for each memcached server; 
	 * For each memcached server create a thread pool of synchronous clients
	 * with specified number of threads (numThreadsPTP)
	 * start a thread of asynchronous client instance for each memcached server
	 * put the ManageQueue instance in hashMap based on the server IP:Port key
	 * @throws IOException 
	 * 
	 */
	private void setupInternalStructure() throws IOException{
		
		this.delegateToQueue = new ConcurrentHashMap<String, ManageQueue>();
		
		// create separate set queue and get thread pool for each memcachedServer
		for(int i=0; i<this.mcAddresses.size(); i++){
			
			ManageQueue tempQueue = new ManageQueue(this.setQueueSize, this.getQueueSize);
			
			// Sync client threads
			for(int j=0; j<this.numThreadsPTP; j++){
				new Thread(new SyncClient(tempQueue.getQueue, this.mcAddresses.get(i))).start();
			}
			
			AsyncClient temp_async = new AsyncClient(this.mcAddresses, i, this.numReplication ,tempQueue.setQueue);
			new Thread(temp_async, this.mcAddresses.get(i)).start();
			tempQueue.setAsync(temp_async);
			this.delegateToQueue.put(this.mcAddresses.get(i), tempQueue);

		}
	}
	
	/**
	 * process request from memaslap. 
	 * parse the request data: based on get/set/delete command.
	 * hash the key, find the relevant memcached server
	 * and add the request to relevant queue based on 
	 * server identity and command (set/get) 
	 * @param clientRequestForward - holds necessary information
	 * about request
	 * @throws Exception
	 */
	public void processRequest(RequestData clientRequestForward) throws Exception{

		byte[] input = clientRequestForward.data.array();
		String[] inputStr = new String(input, "UTF-8").split(" ");
		
		if(inputStr.length >= 2){
		
			// parsing for set/get/delete
			if(inputStr[0].trim().equals("get")){
				
				// get the memcached server address which will process the request
				String selectedServer = this.consistentHash.get(inputStr[1].trim());
				
				// set the type of operation and time of enqueue
				clientRequestForward.requestType = "GET"; // logging info
				clientRequestForward.set_time_enqueue(); // logging info
				
				// push the request to the GET queue of the selected memcached server
				this.delegateToQueue.get(selectedServer).getQueue.put(clientRequestForward);
			}
			else if(inputStr[0].trim().equals("set") || inputStr[0].trim().equals("delete")){
				
				// set the type of the operation (logging info)
				clientRequestForward.requestType = inputStr[0].trim().toUpperCase(); // logging info
				
				// check for replication
				if(this.numReplication == 1){
					
					// get the memcached server address which will process the request
					String selectedServer = this.consistentHash.get(inputStr[1].trim());
					
					// set the time of enqueue
					clientRequestForward.set_time_enqueue(); // logging info
					// push the request to the SET queue of the selected memcached server 
					this.delegateToQueue.get(selectedServer).setQueue.put(clientRequestForward);
					// wake up the selector of the asynchronous client to process the new request
					this.delegateToQueue.get(selectedServer).getAsync().wakeSelector();
				}
				else if(this.numReplication > 1){
					ArrayList<String> selectedServers = this.consistentHash.getWithReplica(inputStr[1].trim(), this.numReplication);
					// set replication addresses
					clientRequestForward.setReplicaAddress(selectedServers);
					// set the time of enqueue
					clientRequestForward.set_time_enqueue(); // logging info
					// push the request to the SET queue of the selected primary memcached server
					// with information on replication server addresses
					this.delegateToQueue.get(selectedServers.get(0)).setQueue.put(clientRequestForward);
					// wake up the selector of the asynchronous client to process the new request
					this.delegateToQueue.get(selectedServers.get(0)).getAsync().wakeSelector();
				}
			}
		}
		
	}
	
}
