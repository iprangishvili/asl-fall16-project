import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;


public class ManageQueue {
	
	public ArrayBlockingQueue<ClientRequestHandler> setQueue;
	public ThreadPoolExecutor executor;
//	public AsyncClient asyncClient;
	
	public ManageQueue(int capacity, ThreadPoolExecutor executor){
		this.setQueue = new ArrayBlockingQueue<ClientRequestHandler>(capacity, true);
		this.executor = executor;
//		this.asyncClient = asyncClient;
	}

}
