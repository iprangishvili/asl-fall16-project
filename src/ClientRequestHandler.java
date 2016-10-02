import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;


public class ClientRequestHandler {
	
	public Server server;
	public SocketChannel socket;
	public ByteBuffer data;
	public int count;
	public int memcahedPort;
	public String memcachedServer;
	
	public ClientRequestHandler(Server server, SocketChannel socket, ByteBuffer data, int count){
		this.server = server;
		this.socket = socket;
		this.data = data;
		this.count = count;
	}
	
	public void memCachedSetting(String selectedServer){
		this.memcachedServer = selectedServer.split(":")[0];
		this.memcahedPort = Integer.parseInt(selectedServer.split(":")[1]);
	}
	

}
