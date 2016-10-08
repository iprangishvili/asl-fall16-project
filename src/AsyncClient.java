import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;



public class AsyncClient implements Runnable{

	private Selector selector;
	private int replicate;
	
	private LinkedList<ClientRequestHandler> requestList = new LinkedList<ClientRequestHandler>();
	private ArrayBlockingQueue<ClientRequestHandler> setQueue;
	
	private SocketChannel primarySocketChannel;
	private Map<String, SocketChannel> secondaryConnections;
	
	// force socket channel to read memcached response in parts;
	private ByteBuffer readBuffer = ByteBuffer.allocate(8); 
	
	private int replicaResponceCounter = 0;
	private boolean replicaSuccess = true;
	
	private ClientRequestHandler receivedClientH = null;
		
	private byte[] failedResponse;

	/**
	 * 
	 * @param mcAddress
	 * @param primaryIndex
	 * @param replicate
	 * @param setQueue
	 * @throws IOException
	 */
	public AsyncClient(List<String> mcAddress, int primaryIndex, int replicate ,ArrayBlockingQueue<ClientRequestHandler> setQueue) throws IOException{
		this.selector = Selector.open();
		this.setQueue = setQueue;
		this.replicate = replicate;
				
		String curr_server = mcAddress.get(primaryIndex).split(":")[0];
		int curr_port = Integer.parseInt(mcAddress.get(primaryIndex).split(":")[1]);
		
		// create primary socket connections
		System.out.println("Created primary server connection: " + mcAddress.get(primaryIndex));
		this.primarySocketChannel = initiateConnection(curr_server, curr_port);
		
		// if replication, open connection to rest of memcached servers
		if(replicate > 1){
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
	    // convert ByteBuffer to String
	    byte[] buff = new byte[readBuffer.position()];
	    readBuffer.flip();
	    readBuffer.get(buff);
	    String readRes = new String(buff).trim().toLowerCase();
//	    System.out.println("Server said: "+ readRes);
//	    System.out.println(readBuffer.toString());
	    
	    if(this.replicate == 1){
//	    	 synchronized (this.requestList) {
	 	    	ClientRequestHandler clientHandler = this.requestList.poll();
	 			clientHandler.server.send(clientHandler.socket, buff);
//	 		}	
	    }
	    else if(this.replicate > 1){
    		replicaResponceCounter++;

    		// one of replications failed
    		// set flag to false
    		if(!readRes.equals("stored")){
    			replicaSuccess = false;
    			failedResponse = Arrays.copyOf(buff, buff.length);
    		}
    		// if all replications succeded send to the original memaslap
    		// stored responce
	    	if(replicaSuccess && replicaResponceCounter == this.replicate){
//	    		synchronized (this.requestList) {
		 	    	ClientRequestHandler clientHandler = this.requestList.poll();
		 			clientHandler.server.send(clientHandler.socket, buff);
//		 		}
//	    		System.out.println("Replica SUCCESS!!!!!");
	    		
	    		// reset paramteres
	    		replicaResponceCounter = 0;
	    		replicaSuccess = true;
	    	}
	    	else if(replicaResponceCounter == this.replicate){
	    		// if one of replications failed and all replicas responded 
	    		// send back failed to original memaslap client and delete request
	    		// from local requestList list
	    		
//	    		synchronized (this.requestList) {
	    			ClientRequestHandler clientHandler = this.requestList.poll();
		 			clientHandler.server.send(clientHandler.socket, failedResponse);
//		 		}
	    		System.out.println("Replica FAILED!!!!!");

	    		// reset paramteres
	    		replicaResponceCounter = 0;
	    		replicaSuccess = true;
	    		
	    	}
	    }
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

//		synchronized (this.requestList) {
			socketChannel.write(requestList.peekLast().data);
			requestList.peekLast().data.rewind();
			key.interestOps(SelectionKey.OP_READ);	
//		}
		

	}
	
	
	public void run(){
		try {
	        while (true){
	        	
	        	if((receivedClientH = this.setQueue.poll()) != null){
	        		
	        		this.primarySocketChannel.keyFor(this.selector).interestOps(SelectionKey.OP_WRITE);

//	        		synchronized (this.requestList) {
						requestList.offer(receivedClientH);
//					}
//	        		 replicate to rest of the memcached servers
	        		if(this.replicate > 1){
	        			for(int i = 1; i<receivedClientH.replicateMcAddress.size(); i++){
	        				this.secondaryConnections.get(receivedClientH.replicateMcAddress.get(i)).keyFor(this.selector).interestOps(SelectionKey.OP_WRITE);

	        			}
	        		}
	        	}       		
	        		
	            int numChannels = this.selector.selectNow();
	            if(numChannels == 0) continue;
	            Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();

	            while (keys.hasNext()){
	                SelectionKey key = keys.next();
	                keys.remove();
	                
	                if (!key.isValid()) continue;

	                if (key.isConnectable()){
	                    this.finishConnect(key);
	                }   
	                else if (key.isWritable()){
	                    this.write(key);
	                }
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
