import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.*;

public class Server extends Thread{

  // parameters
  
  // Global Variables
  private Selector selector;
  private ServerSocketChannel serverSocketChannel;
  private Middleware middleware;
  
  private List<SocketChangeRequestInfo> pendingChanges = new LinkedList<SocketChangeRequestInfo>();
  private Map<SocketChannel,ByteBuffer> pendingData = new HashMap<SocketChannel,ByteBuffer>();
  
  private int PORT;
  private String hostAddress;
  
  private ByteBuffer echoBuffer = ByteBuffer.allocate(2024);

  
  public Server(String myIp, int myPort, List<String> mcAddresses,int numThreadsPTP,int writeToCount) throws IOException{
	  this.PORT = myPort;
	  this.hostAddress = myIp;
	  this.middleware = new Middleware(mcAddresses, numThreadsPTP, writeToCount);
	  this.selector = initSelector();	 
	  
  }
  
  
  /**
   * initialize server socket channel and selector
   * @return selector
   * @throws IOException
   */
  private Selector initSelector() throws IOException{
	  
	  // open a socket
	this.serverSocketChannel = ServerSocketChannel.open();
	// non-blocking socket configuration
	serverSocketChannel.configureBlocking(false);
	// bind socket to specified IP and port
	InetSocketAddress address = new InetSocketAddress(this.hostAddress, this.PORT);
	serverSocketChannel.socket().bind(address);
	
	System.out.println("server started on PORT: " + this.PORT);
	
	
	Selector newselector = Selector.open();
	serverSocketChannel.register(newselector, SelectionKey.OP_ACCEPT);
	
	  return newselector;
  }
  
  
  /**
   * accept new connection from client (memaslap)
   * and register read action on selection key
   * @param key - SelectionKey with accept action
   * @throws IOException
   */
  private void accept(SelectionKey key) throws IOException{
	  
//	  System.out.println("Connection has been accepted");
	  // new server socket channel            
	  ServerSocketChannel curr_serverSocketChannel = (ServerSocketChannel) key.channel();
	  // client socketChannel
	  SocketChannel clientSocket = curr_serverSocketChannel.accept();
	  if(clientSocket != null){
	  	clientSocket.configureBlocking(false);
	      // Add the new connection to the selector = read
	  	clientSocket.register(this.selector, SelectionKey.OP_READ);
	  }
  }
  
  /**
   * read the data sent from client (memaslap)
   * register selection key with no action
   * and invoke processRequest method from Middleware
   * @param currentKey selection key with read action
   * @return String representation of client data
   * @throws Exception 
   */
  private void read(SelectionKey currentKey) throws Exception{
	  
	  echoBuffer.clear();
	  // Read the data
	  SocketChannel sc = (SocketChannel) currentKey.channel();
      int code = 0;
      byte[] buff;
      try{
    	  
    	  code = sc.read(echoBuffer);
    	  buff = new byte[echoBuffer.position()];
          echoBuffer.flip();
          echoBuffer.get(buff);
      }
      catch(IOException e){
    	  
    	  currentKey.cancel();
    	  sc.close();
    	  return;
      }
      // on socket disconnect
      if (code == -1) {
    	  // Remote entity shut the socket down
    	  sc.close();
    	  currentKey.cancel();
    	  return;
      }
      

	  currentKey.interestOps(0);
	  RequestData forward_request = new RequestData(this, sc, ByteBuffer.wrap(buff));
	  forward_request.set_request_receive_time();
	  this.middleware.processRequest(forward_request);
  }
  
  /**
   * create a selectionKey action Change request to write
   * and add to pendingChanges list
   * add data to pendingData hashmap for specified socket channel
   * @param socket - socket channel
   * @param data - data to send to memaslap (received from memcached server)
   * @throws IOException
   */
  public void send(SocketChannel socket, byte[] data) throws IOException {
//	  System.out.println("Sending Data");
	  
		synchronized (this.pendingChanges) {

			synchronized (this.pendingData) {
				this.pendingChanges.add(new SocketChangeRequestInfo(socket, SocketChangeRequestInfo.CHANGEOPS, SelectionKey.OP_WRITE));
				
				this.pendingData.put(socket, ByteBuffer.wrap(data));
				this.selector.wakeup();
				
				
			}
		}
	  	  
	}
  
	/**
	 * write to client (memaslap)
	 * and register selection key action to read
	 * @param key - selection key with write action
	 * @throws IOException
	 */
  private void write(SelectionKey key) throws IOException {
//	  System.out.println("Writing Data to memaslap");
		SocketChannel socketChannel = (SocketChannel) key.channel();

		synchronized (this.pendingData) {
			ByteBuffer buf = this.pendingData.get(socketChannel);
			socketChannel.write(buf);
			key.interestOps(SelectionKey.OP_READ);
		}
	}

  
  public void run(){
	  
	  while(true){
		  try{
			  
			// Process any pending changes
				synchronized (this.pendingChanges) {
					Iterator<SocketChangeRequestInfo> changes = this.pendingChanges.iterator();
					while (changes.hasNext()) {
						SocketChangeRequestInfo change = (SocketChangeRequestInfo) changes.next();
						switch (change.type) {
						case SocketChangeRequestInfo.CHANGEOPS:
							SelectionKey key = change.socket.keyFor(this.selector);							
							key.interestOps(change.ops);
						}
					}
					this.pendingChanges.clear();
				}
			  
			  
			  int numChannel = this.selector.select();
			  if(numChannel == 0) continue;
			  
			  Set<SelectionKey> selectedKeys = this.selector.selectedKeys();
			  Iterator<SelectionKey> it = selectedKeys.iterator();
			  
			  while(it.hasNext()){
				  SelectionKey key = (SelectionKey) it.next();
				  it.remove();
				  if(!key.isValid()) continue;
				  
				  if(key.isAcceptable()){
					  this.accept(key);
				  }
				  else if(key.isReadable()){
					  this.read(key);
				  }
				  else if(key.isWritable()){
					  this.write(key);
				  }
			  }
		  }
		  catch(Exception e){
			  // if one of the clients (memaslap) disconnects badly;
			  // clear all change data
	    	  // not handling it properly yet; Find out if it needs to be handled at all;
	    	  this.pendingChanges.clear();
	    	  this.pendingData.clear();
			  e.printStackTrace();
		  }
	  }
  }
  
}
