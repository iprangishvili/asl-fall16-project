import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;



public class AsyncClient implements Runnable{

	private Selector selector;
	private int replicate;
	
	private LinkedList<RequestData> requestList = new LinkedList<RequestData>();
	private ArrayBlockingQueue<RequestData> setQueue;
	
	private SocketChannel primarySocketChannel;
	private Map<String, SocketChannel> secondaryConnections;
	
	private Map<SocketChannel, Integer> requestTracker;
	
	// force socket channel to read memcached response in parts;
	private ByteBuffer readBuffer = ByteBuffer.allocate(1024); // !!!  important 
	private ByteArrayOutputStream bout = new ByteArrayOutputStream();

	private RequestData receivedClientH = null;
	
		
	/**
	 * 
	 * @param mcAddress
	 * @param primaryIndex
	 * @param replicate
	 * @param setQueue
	 * @throws IOException
	 */
	public AsyncClient(List<String> mcAddress, int primaryIndex, int replicate ,ArrayBlockingQueue<RequestData> setQueue) throws IOException{
		this.selector = Selector.open();
		this.setQueue = setQueue;
		this.replicate = replicate;
		System.out.println("replication #: " + this.replicate);
				
		String curr_server = mcAddress.get(primaryIndex).split(":")[0];
		int curr_port = Integer.parseInt(mcAddress.get(primaryIndex).split(":")[1]);
				
		// create primary socket connections
		System.out.println("Created primary server connection: " + mcAddress.get(primaryIndex));
		this.primarySocketChannel = initiateConnection(curr_server, curr_port);
		
		// if replication, open connection to rest of memcached servers
		if(replicate > 1){
			requestTracker = new HashMap<SocketChannel, Integer>();
			requestTracker.put(this.primarySocketChannel, 0);
			initiateSecondaryConnection(mcAddress, primaryIndex);
		}
	}
	
	/**
	 * initiate connection to specified memcached server and port
	 * open socket channel
	 * @throws IOException
	 */
	private SocketChannel initiateConnection(String memServer, int memPort) throws IOException{
		SocketChannel curr_channel = SocketChannel.open();
		curr_channel.configureBlocking(false);
	
		// Kick off connection establishment
		curr_channel.connect(new InetSocketAddress(memServer, memPort));
		
		curr_channel.register(this.selector, SelectionKey.OP_CONNECT);
		return curr_channel;
	}
	
	private void initiateSecondaryConnection(List<String> mcAddress, int primaryIndex) throws IOException{
		this.secondaryConnections = new HashMap<String, SocketChannel>();
		String curr_server;
		int curr_port;
		for(int i=0; i< mcAddress.size(); i++){
			if(i != primaryIndex){
				curr_server = mcAddress.get(i).split(":")[0];
				curr_port = Integer.parseInt(mcAddress.get(i).split(":")[1]);
				this.secondaryConnections.put(mcAddress.get(i),initiateConnection(curr_server, curr_port));
				requestTracker.put(this.secondaryConnections.get(mcAddress.get(i)), 0);
				System.out.println("Created connection to server: " + mcAddress.get(i));
			}
		}
		
	}
	
	
	private void finishConnect(SelectionKey key){
		SocketChannel socketChannel = (SocketChannel) key.channel();
		
		// Finish the connection. If the connection operation failed
		// this will raise an IOException.
		try {
			socketChannel.finishConnect();
			key.interestOps(0);
		} catch (IOException e) {

			// Cancel the channel's registration with our selector
			System.out.println(e);
			key.cancel();
			return;
		}
	
	}
		
	 
	private void close(){
	    try {
	        selector.close();
	    } catch (IOException e) {
	        // TODO Auto-generated catch block
	        e.printStackTrace();
	    }
	}
	
	/**
	 * read data sent from memcached server, 
	 * get from requestList information (server instance,
	 * socket channel), send the read data to memaslap using that 
	 * information. At last delete socket channel key from requestList 
	 * map, close the socket channel and cancel the key
	 * @param key
	 * @throws IOException
	 */
	private void read (SelectionKey key) throws IOException {
	    SocketChannel channel = (SocketChannel) key.channel();
	    readBuffer.clear();	
	    bout.reset();
	    int length;
	    try{
	    	length = channel.read(readBuffer);
	    } catch (IOException e){
	        System.out.println("Reading problem, closing connection");
	        key.cancel();
	        channel.close();
	        return;
	    }
	    if (length == -1){
	        System.out.println("Nothing was read from server");
	        channel.close();
	        key.cancel();
	        return;
	    }	    
	    // convert ByteBuffer to byte array
//	    byte[] buff = new byte[readBuffer.position()];
//	    readBuffer.flip();
//	    readBuffer.get(buff);
	    readBuffer.flip();
	    byte curr_char;
	    for(int i=0; i<length; i++){
	    	curr_char = readBuffer.get(i);
	    	bout.write(curr_char);
	    }	  
	    
//	    if(length == 0){
//	    	System.out.println("empty");
//	    }
	    
	    if(this.replicate == 1){
	 		handleMultipleResponse(bout.toString().split("\n"));
	    }
	    else if(this.replicate > 1){
	    	handleMultipleResponseWithReplica(bout.toString().split("\n"), channel);
	    }
//	    key.interestOps(0);
	}
	
	/**
	 * get data from requestList based on specific socket channel
	 * write data to memcached server and
	 * register selection key with READ action
	 * @param key
	 * @throws IOException
	 */
	private void write(SelectionKey key) throws IOException {
		
		SocketChannel socketChannel = (SocketChannel) key.channel();
		
		requestList.peekLast().set_time_server_send(); // logging data TODO: adjust checking for the case of replication for logging this info
		socketChannel.write(requestList.peekLast().data);
		requestList.peekLast().data.rewind();
		key.interestOps(SelectionKey.OP_READ);			

	}
	
	private void handleMultipleResponse(String[] responses) throws IOException{
//		String responses[] = res.split("\n");
//		System.out.println("length: " + responses.length + "read: " + res);
		for(int i=0; i<responses.length; i++){
			RequestData clientHandler = requestList.poll();
			clientHandler.calculate_T_server(); // logging data
			
			// check for delete action 
			if(clientHandler.requestType.equals("DELETE")){
				// if delete operation failed
				if(!responses[i].trim().toLowerCase().equals("deleted")){
					clientHandler.set_success_flag(false);
				}
			}
			else if(!responses[i].trim().toLowerCase().equals("stored")){ // set action failed
				System.out.println("fail: " + responses[i]);
				clientHandler.set_success_flag(false);
			}
			
			clientHandler.setResponse(ByteBuffer.wrap((responses[i] + "\n").getBytes()));
			clientHandler.server.send(clientHandler);
		}
	}
	
	private void handleMultipleResponseWithReplica(String[] responses, SocketChannel ch) throws IOException{
//		String responses[] = res.split("\n");
//		System.out.println("List size: " + this.requestList.size());
		int requestListIndex = requestTracker.get(ch);
		for(int i=0; i<responses.length; i++){
				RequestData clientHandler = requestList.get(requestListIndex);
				requestListIndex++;	
				clientHandler.incrementReplicaCounter();
				
				// check if action is delete
				if(clientHandler.requestType.equals("DELETE")){
					// check if delete operation succeded
					if(!responses[i].trim().toLowerCase().equals("deleted")){
						clientHandler.set_success_flag(false);
						clientHandler.setResponse(ByteBuffer.wrap((responses[i] + "\n").getBytes()));
					}
				}
				else{	
					// if set failed
					if(!responses[i].trim().toLowerCase().equals("stored")){
						System.out.println("fail: " + responses[i]);
						clientHandler.set_success_flag(false);
						clientHandler.setResponse(ByteBuffer.wrap((responses[i] + "\n").getBytes()));
					}
				}
				
				if(clientHandler.get_success_flag()){
					clientHandler.setResponse(ByteBuffer.wrap((responses[i] + "\n").getBytes()));
				}
				
				if(clientHandler.getReplicaCounter() == this.replicate){	
//					requestTracker.put(ch, requestListIndex);
					decrementCounter(ch);
					requestListIndex = requestTracker.get(ch);
					clientHandler.calculate_T_server(); // logging data;
					clientHandler.server.send(requestList.poll());
				}
		}
		requestTracker.put(ch, requestListIndex);
	}
	
	private void decrementCounter(SocketChannel ch){
		Iterator<Entry<SocketChannel, Integer>> scIterator = requestTracker.entrySet().iterator();
		Entry<SocketChannel, Integer> curr_entry;
		while(scIterator.hasNext()){
			curr_entry = scIterator.next();
			if(!ch.equals(curr_entry.getKey()) && curr_entry.getValue() != 0){
				requestTracker.put(curr_entry.getKey(), curr_entry.getValue()-1);
			}
		}
	}
	
	public void wakeSelector(){
		this.selector.wakeup();
	}
	
	
	public void run(){
		try {
	        while (true){
	        	
	        	if((receivedClientH = this.setQueue.poll()) != null){
	        		receivedClientH.calculate_T_queue();
//	        		this.primarySocketChannel.keyFor(this.selector).interestOps(SelectionKey.OP_WRITE);
	        		
	        		receivedClientH.set_time_server_send(); // logging data TODO: adjust checking for the case of replication for logging this info
	        		this.primarySocketChannel.write(receivedClientH.data);
	        		this.primarySocketChannel.keyFor(this.selector).interestOps(SelectionKey.OP_READ);

	        		receivedClientH.data.rewind();
	        		// replicate to rest of the memcached servers
	        		if(this.replicate > 1){
	        			for(int i = 1; i<receivedClientH.replicateMcAddress.size(); i++){
//	        				this.secondaryConnections.get(receivedClientH.replicateMcAddress.get(i)).keyFor(this.selector).interestOps(SelectionKey.OP_WRITE);
	        				this.secondaryConnections.get(receivedClientH.replicateMcAddress.get(i)).write(receivedClientH.data);
	        				this.secondaryConnections.get(receivedClientH.replicateMcAddress.get(i)).keyFor(this.selector).interestOps(SelectionKey.OP_READ);
	        				receivedClientH.data.rewind();
	        			}
	        		}
					requestList.offer(receivedClientH);
	        	}       		
	        	
	        		
	            int numChannels = this.selector.select();
	            if(numChannels == 0) continue;
	            Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();

	            while (keys.hasNext()){
	                SelectionKey key = keys.next();
	                keys.remove();
	                
	                if (!key.isValid()) continue;

	                if (key.isConnectable()){
	                    this.finishConnect(key);
	                }   
//	                else if (key.isWritable()){
//	                    this.write(key);
//	                }
	                else if (key.isReadable()){
	                    this.read(key);
	                }
	                

	            }   
	        }
	    } catch (IOException e1) {
	        // TODO Auto-generated catch block
	        e1.printStackTrace();
	    } finally {
	        close();
	    }
	}
	
}