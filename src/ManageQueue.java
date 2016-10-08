import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;


public class ManageQueue {
	
	public ArrayBlockingQueue<ClientRequestHandler> setQueue;
	public ArrayBlockingQueue<ClientRequestHandler> getQueue;

	public ManageQueue(int capacity, int getCapacity){
		this.setQueue = new ArrayBlockingQueue<ClientRequestHandler>(capacity, true);
		this.getQueue = new ArrayBlockingQueue<ClientRequestHandler>(getCapacity);
	}

}
