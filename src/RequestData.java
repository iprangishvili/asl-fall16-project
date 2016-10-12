import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;


public class RequestData {
	
	public Server server;
	public SocketChannel socket;
	public ByteBuffer data;
	public ArrayList<String> replicateMcAddress;
	public String requestType = "";
	
	private ByteBuffer response; // response from memcached
	
	// logging information
	private long time_received_request; // time request was received
	private long time_of_enqueue; // time request of put in queue
	private long time_request_server_send; // time request was send to server
	private boolean success_flag; // whether request succeded
	
	private long T_mw;
	private long T_queue;
	private long T_server;
	
	public RequestData(Server server, SocketChannel socket, ByteBuffer data){
		this.server = server;
		this.socket = socket;
		this.data = data;
	}
	
	/**
	 * set response - received from memcached server
	 * @param res
	 */
	public void setResponse(ByteBuffer res){
		this.response = res;
	}
	
	/**
	 * get response - received from memcached server
	 * @return
	 */
	public ByteBuffer getResponse(){
		return this.response;
	}
	
	/**
	 * set replication memcached addresses
	 * @param replicateMcAddress
	 */
	public void setReplicaAddress(ArrayList<String> replicateMcAddress){
		this.replicateMcAddress = replicateMcAddress;
	}
	
	/**
	 * set time request was received
	 */
	public void set_request_receive_time(){
		this.time_received_request = System.nanoTime();
	}

	/**
	 * set time when request was enqueued
	 */
	public void set_time_enqueue(){
		this.time_of_enqueue = System.nanoTime();
	}
	
	/**
	 * set time request was send to server
	 */
	public void set_time_server_send(){
		this.time_request_server_send = System.nanoTime();
	}
	
	/**
	 * calculate Time spent between receiving a request and sending 
	 * back the response; (millisecond)
	 */
	public void calculate_T_mw(){
		this.T_mw = (System.nanoTime() - this.time_received_request);
	}
	
	/**
	 * calculate Time each request spends in a queue 
	 * (Time from enqueue to time of dequeue)
	 * (milliseconds)
	 */
	public void calculate_T_queue(){
		this.T_queue = (System.nanoTime() - this.time_of_enqueue);
	}
	
	/**
	 *  calculate Time each request spend in the server 
	 *  (time from sending request to the server 
	 *  to time of receiving server response)
	 *  (milliseconds)
	 */
	public void calculate_T_server(){
		this.T_server = (System.nanoTime() - this.time_request_server_send);
	}
	
	/**
	 * get Time spent between receiving a request and sending 
	 * back the response; (millisecond)
	 * @return
	 */
	public long get_T_mw(){
		return this.T_mw;
	}
	
	/**
	 * get Time each request spends in a queue 
	 * (Time from enqueue to time of dequeue)
	 * (milliseconds)
	 * @return
	 */
	public long get_T_queue(){
		return this.T_queue;
	}
	
	/**
	 *  get Time each request spend in the server 
	 *  (time from sending request to the server 
	 *  to time of receiving server response)
	 *  (milliseconds)
	 * @return
	 */
	public long get_T_server(){
		return this.T_server;
	}
	
	/** 
	 * set flag whether request was successfully executed
	 * @param flag
	 */
	public void set_success_flag(boolean flag){
		this.success_flag = flag;
	}
	
	/**
	 * get request success flag
	 * @return
	 */
	public boolean get_success_flag(){
		return this.success_flag;
	}
}
