import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;


public class ClientRequestHandler {
	
	public Server server;
	public SocketChannel socket;
	public ByteBuffer data;
	public ArrayList<String> replicateMcAddress;

	
	public ClientRequestHandler(Server server, SocketChannel socket, ByteBuffer data){
		this.server = server;
		this.socket = socket;
		this.data = data;
	}
	
	public void setReplicaAddress(ArrayList<String> replicateMcAddress){
		this.replicateMcAddress = replicateMcAddress;
	}

}
