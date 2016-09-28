import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;


public class Middleware implements Runnable{
	
	private AsyncClient asyncClient;
	private List<ClientRequestHandler> queue = new LinkedList<ClientRequestHandler>();

	/**
	 * TODO: add parameters
	 * @throws IOException 
	 */
	public Middleware(AsyncClient asyncClient) throws IOException{
		// initiate client connection to memcached server
		this.asyncClient = asyncClient;
		new Thread(asyncClient).start();
	}
	
	public void parseRequest(String input){
		System.out.println("parse request");
		String action;
		String splitData[] = input.split(" ");
		if(splitData.length > 0){
			action = splitData[0];
			if(action.equals("get")){
				if(splitData.length == 2){
					String get_hashKey = splitData[1];
					// TODO: process hash key determine the server; Consisten Hashing
					System.out.println("Action: Get; Key: " + get_hashKey);
				}
			}
			else if(action.equals("set")){
				System.out.println("Action: Set " + splitData.length);
				if(splitData.length == 5){
					String set_key = splitData[1];
					int flag = Integer.parseInt(splitData[2]);
					int exp_time = Integer.parseInt(splitData[3]);
					String valueParse[] = splitData[4].split("\\r?\\n");
					if(valueParse.length == 2){
						int set_value_size = Integer.parseInt(valueParse[0]);
						String set_value = valueParse[1];
						System.out.println("parsed value: " + set_value);
						System.out.println("parsed value: " + set_value_size);
					}
				}
				// TODO: process hash key determine the server; Consistent Hashing
			}
		}
	}
	
	public void processRequest(Server server, SocketChannel socket, byte[] input, int count) throws Exception{
		//TODO: process hash key; consistent hashing;
		
//		asyncClient.sendToMemCache(server, socket, input);
		ClientRequestHandler clientRequestForward = new ClientRequestHandler(server, socket, ByteBuffer.wrap(input));
		asyncClient.sendToMemCache(clientRequestForward);

//		synchronized(queue) {
//			queue.add(new ClientRequestHandler(server, socket, ByteBuffer.wrap(input)));
//			queue.notify();
//		}
	}
	
	public void run(){
//		ClientRequestHandler clientRequestForward;
//		
//		while(true) {
//			// Wait for data to become available
//			synchronized(queue) {
//				while(queue.isEmpty()) {
//					try {
//						queue.wait();
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
//				clientRequestForward = (ClientRequestHandler) queue.remove(0);
//				// send to client
//				try {
//					asyncClient.sendToMemCache(clientRequestForward);
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//			
//			
//		}
		
		// TODO: thread pooling; waiting queue; mitigate sendToMemcache here
		
	}
	
	
}
