import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AsyncClientSocketChannelHanlder {
	
	private SocketChannel primaryChannel;
	private Map<String, SocketChannel> secondaryChannels;
	private RequestData rdata;
	private int replicationCounter;
	private boolean replicationSuccess;
	private int numReplica;
	private byte[] failedResponse;
	
	public AsyncClientSocketChannelHanlder(SocketChannel primaryChannel, int numReplica){
		this.primaryChannel = primaryChannel;
		this.secondaryChannels = new HashMap<String, SocketChannel>();
		
		this.replicationCounter = 0;
		this.replicationSuccess = true;
		
		this.numReplica = numReplica;
	}
	
	public void setRequestData(RequestData rdata){
		this.rdata = rdata;
	}
	
	public RequestData getRequestData(){
		return this.rdata;
	}
	
	public void addSecondaryChannel(String key, SocketChannel value, Selector selector){
		this.secondaryChannels.put(key, value);
		this.secondaryChannels.get(key).keyFor(selector).attach(this);

	}
	
	
	public void setPrimaryKeyWrite(Selector selector){
		
		this.primaryChannel.keyFor(selector).interestOps(SelectionKey.OP_WRITE);
	}
	
	public void setSecondaryKeyWrite(Selector selector){
		
		for(int i = 1; i<this.rdata.replicateMcAddress.size(); i++){
			this.secondaryChannels.get(this.rdata.replicateMcAddress.get(i)).keyFor(selector).interestOps(SelectionKey.OP_WRITE);
		}
	}
	
	public void keyAttachPrimary(Selector selector){
		this.primaryChannel.keyFor(selector).attach(this);
	}
	
	public void dealWithReplication(byte[] response) throws IOException{
		this.replicationCounter++;
		
		if(!new String(response).trim().toLowerCase().equals("stored")){
			this.replicationSuccess = false;
			this.failedResponse = response;
		}
		
		if(this.replicationCounter == this.numReplica && this.replicationSuccess){
//			System.out.println("Success!!!: " + new String(response));
			
			this.rdata.calculate_T_server();// logging info 
			this.rdata.set_success_flag(true); // logging info
			
			this.rdata.setResponse(ByteBuffer.wrap(response));
			this.rdata.server.send(this.rdata);
			this.replicationCounter = 0;
			this.replicationSuccess = true;
		}
		else if(this.replicationCounter == this.numReplica && !this.replicationSuccess){
			System.out.println("FAIL!!!");
			
			this.rdata.calculate_T_server();// logging info 
			this.rdata.set_success_flag(false); // logging info
			
			this.rdata.setResponse(ByteBuffer.wrap(response));
			this.rdata.server.send(this.rdata);
			this.replicationCounter = 0;
			this.replicationSuccess = true;
		}
		
	}
	

}
