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



public class AsyncClient implements Runnable{

	private Selector selector;
	private Map<SocketChannel,List<ClientRequestHandler>> requestList = new HashMap<SocketChannel,List<ClientRequestHandler>>();
	private LinkedList<SocketChangeRequestInfo> pendingChanges = new LinkedList<SocketChangeRequestInfo>();
	
	public AsyncClient() throws IOException{
		this.selector = Selector.open();
	}
	
	
	public void run(){
		try {
	        while (true){
	        	
	        	// Process any pending changes
				synchronized (this.pendingChanges) {
					Iterator<SocketChangeRequestInfo> changes = this.pendingChanges.iterator();
					while (changes.hasNext()) {
						SocketChangeRequestInfo change = (SocketChangeRequestInfo) changes.next();
						switch (change.type) {
						case SocketChangeRequestInfo.CHANGEOPS:
							SelectionKey key = change.socket.keyFor(this.selector);
							key.interestOps(change.ops);
							break;
						case SocketChangeRequestInfo.REGISTER:
							change.socket.register(this.selector, change.ops);
							break;
						}
					}
					this.pendingChanges.clear();
				}

	            this.selector.select();
	             
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
	    
	    ClientRequestHandler clientHandler = (ClientRequestHandler) key.attachment();
	    if(clientHandler != null){
	    	
//	    	System.out.println("Sending memcached response back to memaslap");
//	    	key.attach(null);
	    	channel.close();
			key.cancel();
			
	    	clientHandler.server.send(clientHandler.socket, (byte[])readBuffer.array());
	    }
	    else{
	    	System.out.println("CLIENT HANDLER IS NULL (BAD BEHAVIOUR)!!!!");
	    }
	    
	}
	
	private void write(SelectionKey key) throws IOException {
//		System.out.println("writing to memcached...");
		
		SocketChannel socketChannel = (SocketChannel) key.channel();

		synchronized (this.requestList) {
			List<ClientRequestHandler> queue = (List<ClientRequestHandler>) this.requestList.get(socketChannel);
			if(queue == null){
				System.out.println("this is the error!!!!");
			}
			if(!queue.isEmpty()){
				ClientRequestHandler tempClient = (ClientRequestHandler) queue.get(0);
				key.attach(tempClient);
//				System.out.println("Client request handler: " + tempClient.server + " channel: " + tempClient.socket);
			}
			// Write until there's not more data ...
			while (!queue.isEmpty()) {
				ClientRequestHandler clientRH = (ClientRequestHandler) queue.get(0);
//				System.out.println("setting loop: " + clientRH.data);

				socketChannel.write(clientRH.data);
				if (clientRH.data.remaining() > 0) {
					// ... or the socket's buffer fills up
					break;
				}
				queue.remove(0);
			}
			

			if (queue.isEmpty()) {
//				System.out.println("setting read");
				// We wrote away all data, so we're no longer interested
				// in writing on this socket. Switch back to waiting for
				// data.
				key.interestOps(SelectionKey.OP_READ);
			}
			
		}
	}
	
	public void sendToMemCache(ClientRequestHandler clientHandler) throws IOException{
	
		// Create a non-blocking socket channel
		SocketChannel socketChannel = SocketChannel.open();
		socketChannel.configureBlocking(false);
	
		// Kick off connection establishment
		socketChannel.connect(new InetSocketAddress("127.0.0.1", 8000));
	
		// Queue a channel registration since the caller is not the 
		// selecting thread. As part of the registration we'll register
		// an interest in connection events. These are raised when a channel
		// is ready to complete connection establishment.
		
		synchronized(this.pendingChanges) {
			this.pendingChanges.add(new SocketChangeRequestInfo(socketChannel, SocketChangeRequestInfo.REGISTER, SelectionKey.OP_CONNECT));
			// And queue the data we want written
			synchronized (this.requestList) {
				List<ClientRequestHandler> queueT = (List<ClientRequestHandler>) this.requestList.get(socketChannel);
				if (queueT == null) {
					queueT = new ArrayList<ClientRequestHandler>();
					this.requestList.put(socketChannel, queueT);
				}
				queueT.add(clientHandler);
			}
		}
		
		
		this.selector.wakeup();
		
	}

}
