import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
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
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class Middleware{
		
	// locally specified variables
	private int setQueueSize = 1000; // queue size for set
	private int getQueueSize = 1000; // queue size for get
	private int numVirtualNodes = 200; // number of virtual nodes for servers (uniform key space distribution)

	// user specified variables at runtime
	private List<String> mcAddresses; // memcached server addresses
	private int numThreadsPTP; // maximum number of threads in Thread pool
	private int numReplication; // number of replication for set
	
	private ConcurrentHashMap<String, ManageQueue> delegateToQueue; // hashmap of <server IP, manageQueue> for storing server specific queue
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
		
		// create hash key for servers
		// replicate virtual nodes for uniform distribution of key space
		// TODO: explore uniform distribution of key space more 
		// look into if current strategy works
		this.consistentHash = new ConsistentHash(this.numVirtualNodes, this.mcAddresses);
		
		// create an internal structure of middleware
		setupInternalStructure();
		
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
	 * process request from memaslap. 
	 * parse the request data: based on get/set command.
	 * hash the key, find the relevant memcached server
	 * and add the request to relevant queue based on 
	 * server identity and command (set/get) 
	 * @param clientRequestForward - holds necessary information
	 * about request
	 * @throws Exception
	 */
	public void processRequest(RequestData clientRequestForward) throws Exception{

		// initial parsing for set/get
		byte[] input = clientRequestForward.data.array();
		String[] inputStr = new String(input, "UTF-8").split(" ");
		
		if(inputStr.length >= 2){
			
			// add request to relevant queue
			if(inputStr[0].equals("get")){
				// get the memcached server address to which the request key belongs
				String selectedServer = this.consistentHash.get(inputStr[1].trim());
				
				// set the time of enqueue
				clientRequestForward.requestType = "GET"; // logging info
				clientRequestForward.set_time_enqueue(); // logging info
				this.delegateToQueue.get(selectedServer).getQueue.put(clientRequestForward);
			}
			else if(inputStr[0].equals("set")){	
				clientRequestForward.requestType = "SET"; // logging info
				// check for replication
				if(this.numReplication == 1){
					
					String selectedServer = this.consistentHash.get(inputStr[1].trim());
					
					// set the time of enqueue
					clientRequestForward.set_time_enqueue(); // logging info
					this.delegateToQueue.get(selectedServer).setQueue.put(clientRequestForward);
				}
				else if(this.numReplication > 1){
					ArrayList<String> selectedServers = new ArrayList<String>(this.consistentHash.getWithReplica(inputStr[1].trim(), this.numReplication));
					// set replication addresses
					clientRequestForward.setReplicaAddress(selectedServers);
					// set the time of enqueue
					clientRequestForward.set_time_enqueue();
					this.delegateToQueue.get(selectedServers.get(0)).setQueue.put(clientRequestForward);
				}
			}
		}
		
	}
	
}
