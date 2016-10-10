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
import java.util.logging.Logger;



public class AsyncClient implements Runnable{

	private Selector selector;
	private int replicate; // number of replications
	
	private ArrayBlockingQueue<RequestData> setQueue;
	// key - socket channel associated with memaslap client 
	// value - asyncronious client socket channel to communicate to memcached
	// each memaslap client gets one async socket channel
	private Map<SocketChannel, AsyncClientSocketChannelHanlder> clientSockets;
	
	private List<String> mcAddress; // list of memcached server IP:PORT
	private int primaryIndex; // index of primary memcached server in mcAddress list
	
	// force socket channel to read memcached response in parts;
	private ByteBuffer readBuffer = ByteBuffer.allocate(2024); 
	private byte[] buff;
	
	private RequestData receivedClientH = null;
	private AsyncClientSocketChannelHanlder asyncSocketChannelHandler;
		
	/**
	 * 
	 * @param mcAddress
	 * @param primaryIndex
	 * @param replicate
	 * @param setQueue
	 * @throws IOException
	 */
	public AsyncClient(List<String> mcAddress, int primaryIndex, int replicate, ArrayBlockingQueue<RequestData> setQueue) throws IOException{
		this.selector = Selector.open();
		this.setQueue = setQueue;
		this.replicate = replicate;
		this.mcAddress = mcAddress;
		this.primaryIndex = primaryIndex;
		
		this.clientSockets = new HashMap<SocketChannel, AsyncClientSocketChannelHanlder>();
	}
	
	/**
	 * initiate connection to specified memcached server and port
	 * open socket channel
	 * @throws IOException
	 */
	private SocketChannel initiateConnection(String memServer) throws IOException{
		String curr_server = memServer.split(":")[0];
		int curr_port = Integer.parseInt(memServer.split(":")[1]);
		
		SocketChannel curr_channel = SocketChannel.open();
		curr_channel.configureBlocking(false);
	
		// Kick off connection establishment
		curr_channel.connect(new InetSocketAddress(curr_server, curr_port));
		
		curr_channel.register(this.selector, SelectionKey.OP_CONNECT);
		System.out.println("Connection initiated");

		return curr_channel;
	}
	
	private void initiateSecondaryConnection(AsyncClientSocketChannelHanlder asyncHandler) throws IOException{
	
		for(int i=0; i< this.mcAddress.size(); i++){
			if(i != this.primaryIndex){
				
				asyncHandler.addSecondaryChannel(mcAddress.get(i), initiateConnection(this.mcAddress.get(i)), this.selector);
				System.out.println("Created connection to server: " + this.mcAddress.get(i));
			}
		}
		
	}
	
	
	private void finishConnect(SelectionKey key){
		SocketChannel socketChannel = (SocketChannel) key.channel();
		
		// Finish the connection. If the connection operation failed
		// this will raise an IOException.
		try {
			socketChannel.finishConnect();
			key.interestOps(SelectionKey.OP_WRITE);
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
	    buff = new byte[readBuffer.position()];
	    readBuffer.flip();
	    readBuffer.get(buff, 0, length);

	    
//	    System.out.println("Server said: " + readRes + " " + length);
	    AsyncClientSocketChannelHanlder tempClientHandler = (AsyncClientSocketChannelHanlder) key.attachment();
	    if(this.replicate == 1){
	    	tempClientHandler.getRequestData().calculate_T_server(); // logging info
	    	// logging info
	    	if(new String(buff).trim().toLowerCase().equals("stored")){
	    		tempClientHandler.getRequestData().set_success_flag(true);
	    	}
	    	else{
	    		tempClientHandler.getRequestData().set_success_flag(false);
	    	}
	    	tempClientHandler.getRequestData().setResponse(ByteBuffer.wrap(buff));
	    	tempClientHandler.getRequestData().server.send(tempClientHandler.getRequestData());
	    }
	    else if(this.replicate > 1){
	    	tempClientHandler.dealWithReplication(buff);
	    }
	    
	    key.interestOps(0);
	}
	
	/**
	 * get data from requestList based on specific socket channel
	 * write data to memcached server and
	 * register selection key with READ action
	 * @param key
	 * @throws IOException
	 */
	private void write(SelectionKey key) throws IOException {
//		System.out.println("in write");
		SocketChannel socketChannel = (SocketChannel) key.channel();

		AsyncClientSocketChannelHanlder tempcleintSocket = (AsyncClientSocketChannelHanlder) key.attachment();
		tempcleintSocket.getRequestData().set_time_server_send();
		
		socketChannel.write(tempcleintSocket.getRequestData().data);
		tempcleintSocket.getRequestData().data.rewind();
		key.interestOps(SelectionKey.OP_READ);	
		
	}
	
	
	public void run(){
		try {
	        while (true){
	        	
	        	if((receivedClientH = this.setQueue.poll()) != null){
	        		
	        		receivedClientH.calculate_T_queue(); // logging data
	        		
	        		asyncSocketChannelHandler = this.clientSockets.get(receivedClientH.socket);
	        		if(asyncSocketChannelHandler == null){
	        			// create a socket channel for primary memcach
	        			asyncSocketChannelHandler = new AsyncClientSocketChannelHanlder(initiateConnection(this.mcAddress.get(this.primaryIndex)), this.replicate);	
	        			this.clientSockets.put(receivedClientH.socket, asyncSocketChannelHandler); // put the socket channel to hashmap	        			
		        		asyncSocketChannelHandler.setRequestData(receivedClientH);
		        		asyncSocketChannelHandler.keyAttachPrimary(this.selector);
	        			// check for replication
	        			if(this.replicate > 1){
	        				initiateSecondaryConnection(asyncSocketChannelHandler);
	        			}
	        		}
	        		else{
		        		asyncSocketChannelHandler.setRequestData(receivedClientH);
		        		asyncSocketChannelHandler.setPrimaryKeyWrite(this.selector);
		        		if(this.replicate > 1){
		        			asyncSocketChannelHandler.setSecondaryKeyWrite(this.selector);
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
