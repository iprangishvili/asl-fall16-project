import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;


public class ClientRequestHandler {
	
	public Server server;
	public SocketChannel socket;
	public ByteBuffer data;
	public int count;
	
	public ClientRequestHandler(Server server, SocketChannel socket, ByteBuffer data, int count){
		this.server = server;
		this.socket = socket;
		this.data = data;
		this.count = count;
	}

}
