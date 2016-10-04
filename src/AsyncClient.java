import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;



public class AsyncClient implements Runnable{

	private Selector selector;
	private String memcachedServer;
	private int memcachedPort;
	private Map<SocketChannel,ClientRequestHandler> requestList = new HashMap<SocketChannel,ClientRequestHandler>();
//	private LinkedList<SocketChangeRequestInfo> pendingChanges = new LinkedList<SocketChangeRequestInfo>();
	private ArrayBlockingQueue<ClientRequestHandler> setQueue;
	
	private ClientRequestHandler receivedClientH = null;

	
	public AsyncClient(String memcachedServer, int memcachedPort, ArrayBlockingQueue<ClientRequestHandler> setQueue) throws IOException{
		this.selector = Selector.open();
		this.memcachedServer = memcachedServer;
		this.memcachedPort = memcachedPort;
		this.setQueue = setQueue;
	}
	
	
	public void run(){
		try {
	        while (true){
	        	
	        	if((receivedClientH = this.setQueue.poll()) != null){
//	        	try {
//					receivedClientH = this.setQueue.take();
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//	        		System.out.println("AsyncClient for server port: " + Thread.currentThread().getName());
//	        		System.out.println("retreiving from set queue: " + this.setQueue.size());
	        		
	        		// Create a non-blocking socket channel
	        		SocketChannel socketChannel = SocketChannel.open();
	        		socketChannel.configureBlocking(false);
	        	
	        		// Kick off connection establishment
	        		socketChannel.connect(new InetSocketAddress(this.memcachedServer, this.memcachedPort));
	        		
        			// socketChannel to hashmap to retreive later the associated data
	        		// TODO: explore if there is need to use syncronized block for requestList
        			synchronized (this.requestList) {
        				this.requestList.put(socketChannel, receivedClientH);
        			}
	        		socketChannel.register(this.selector, SelectionKey.OP_CONNECT);
//	        		this.selector.wakeup();
	        	}       		
	        		
	        		
	        		
	        		
	        	// Process any pending changes
//				synchronized (this.pendingChanges) {
//					Iterator<SocketChangeRequestInfo> changes = this.pendingChanges.iterator();
//					while (changes.hasNext()) {
//						SocketChangeRequestInfo change = (SocketChangeRequestInfo) changes.next();
//						switch (change.type) {
//						case SocketChangeRequestInfo.CHANGEOPS:
//							SelectionKey key = change.socket.keyFor(this.selector);
//							key.interestOps(change.ops);
//							break;
//						case SocketChangeRequestInfo.REGISTER:
//							change.socket.register(this.selector, change.ops);
//							break;
//						}
//					}
//					this.pendingChanges.clear();
//				}
	        	
	        	

	            int numChannels = this.selector.selectNow();
	            if(numChannels == 0) continue;
	            Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();

	            while (keys.hasNext()){
	                SelectionKey key = keys.next();

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
	                keys.remove();
	            }   
	        }
	    } catch (IOException e1) {
	        // TODO Auto-generated catch block
	        e1.printStackTrace();
	    } finally {
	        close();
	    }
	}
	
	
	private void finishConnect(SelectionKey key){
		SocketChannel socketChannel = (SocketChannel) key.channel();
		
		// Finish the connection. If the connection operation failed
		// this will raise an IOException.
		try {
			socketChannel.finishConnect();
		} catch (IOException e) {
	    	System.out.println("Where is the exception");

			// Cancel the channel's registration with our selector
			System.out.println(e);
			key.cancel();
			return;
		}
	
		// Register an interest in writing on this channel
		key.interestOps(SelectionKey.OP_WRITE);
	}
		
	 
	private void close(){
	    try {
	        selector.close();
	    } catch (IOException e) {
	    	System.out.println("Where is the exception");

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
//		System.out.println("in read function memcached client");
	    SocketChannel channel = (SocketChannel) key.channel();
	    ByteBuffer readBuffer = ByteBuffer.allocate(4024);
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
	    readBuffer.flip();
	    byte[] buff = new byte[4024];
	    readBuffer.get(buff, 0, length);
//	    System.out.println("Server said: "+new String(buff));
	    
	    synchronized (this.requestList) {
			ClientRequestHandler clientHandler = this.requestList.get(channel);
			// delete socketchannel key from hashmap
			this.requestList.remove(channel);
			// close channel/key
			channel.close();
			key.cancel();
			clientHandler.server.send(clientHandler.socket, (byte[])readBuffer.array());
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
//		System.out.println("writing to memcached...");
		
		SocketChannel socketChannel = (SocketChannel) key.channel();

		synchronized (this.requestList) {
			
			ClientRequestHandler clientRH = this.requestList.get(socketChannel);
			socketChannel.write(clientRH.data);
			key.interestOps(SelectionKey.OP_READ);
			
		}
	}
	
//	public void sendToMemCache(ClientRequestHandler clientHandler) throws IOException{
//	
//		// Create a non-blocking socket channel
//		SocketChannel socketChannel = SocketChannel.open();
//		socketChannel.configureBlocking(false);
//	
//		// Kick off connection establishment
//		socketChannel.connect(new InetSocketAddress(this.memcachedServer, this.memcachedPort));
//	
//		// Queue a channel registration since the caller is not the 
//		// selecting thread. As part of the registration we'll register
//		// an interest in connection events. These are raised when a channel
//		// is ready to complete connection establishment.
//		
//		synchronized(this.pendingChanges) {
//			this.pendingChanges.add(new SocketChangeRequestInfo(socketChannel, SocketChangeRequestInfo.REGISTER, SelectionKey.OP_CONNECT));
//			// And queue the data we want written
//			synchronized (this.requestList) {
//
//				this.requestList.put(socketChannel, clientHandler);
//			}
//		}
//		
//		
//		this.selector.wakeup();
//		
//	}

}
