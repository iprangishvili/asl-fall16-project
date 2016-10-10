import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;


public class ManageQueue {
	
	public ArrayBlockingQueue<RequestData> setQueue;
	public ArrayBlockingQueue<RequestData> getQueue;

	public ManageQueue(int capacity, int getCapacity){
		this.setQueue = new ArrayBlockingQueue<RequestData>(capacity, true);
		this.getQueue = new ArrayBlockingQueue<RequestData>(getCapacity);
	}

}
