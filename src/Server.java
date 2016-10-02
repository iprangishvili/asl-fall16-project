import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.*;

public class Server implements Runnable{

  // parameters
  private static int PORT = 11212;
  
  // Global Variables
  private Selector selector;
  private ServerSocketChannel serverSocketChannel;
  private Middleware middleware;
  private List<SocketChangeRequestInfo> pendingChanges = new LinkedList<SocketChangeRequestInfo>();
  private Map<SocketChannel,ByteBuffer> pendingData = new HashMap<SocketChannel,ByteBuffer>();
  
  public Server(Middleware middleware) throws IOException{
	  this.selector = initSelector();
	  this.middleware = middleware;
  }
  
  private Selector initSelector() throws IOException{
	  
	  // open a socket
	this.serverSocketChannel = ServerSocketChannel.open();
	// non-blocking socket configuration
	serverSocketChannel.configureBlocking(false);
	// bind socket to specified port
	serverSocketChannel.socket().bind(new InetSocketAddress(PORT));
	
	System.out.println("server started on PORT: " + PORT);
	
	
	Selector newselector = Selector.open();
	serverSocketChannel.register(newselector, SelectionKey.OP_ACCEPT);
	
	  return newselector;
  }
  
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
   * read the data sent from client
   * @param currentKey selection key with read action
   * @return String representation of client data
 * @throws Exception 
   */
  private void read(SelectionKey currentKey) throws Exception{

	  ByteBuffer echoBuffer = ByteBuffer.allocate(4024);
	  // Read the data
      String clientInput = new String();
	  SocketChannel sc = (SocketChannel) currentKey.channel();
      int code = 0;
      try{
    	  
    	  code = sc.read(echoBuffer);
    	  byte b[] = new byte[echoBuffer.position()];
          echoBuffer.flip();
          echoBuffer.get(b);
          clientInput+=new String(b, "UTF-8");
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
      else{
    	  if(clientInput.length()>1){
    		  clientInput = clientInput.substring(0, clientInput.length()-2);
    	  }
      }
      
     
    	  currentKey.interestOps(0);
		  middleware.processRequest(this, sc, echoBuffer.array(), code);
  }
  
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
  
  public static void main(String args[]){
	    try{
	    	
	    	Middleware middleware = new Middleware();
	    	new Thread(middleware).start();
	    	
	    	Server server = new Server(middleware);
	    	new Thread(server).start();
	    }
	    catch(IOException e){
	      e.printStackTrace();
	    }
	  }
  
}
