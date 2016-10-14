import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;


public class ManageQueue {
	
	public ArrayBlockingQueue<RequestData> setQueue;
	public ArrayBlockingQueue<RequestData> getQueue;
	
	private AsyncClient asyncCLient;

	public ManageQueue(int capacity, int getCapacity){
		this.setQueue = new ArrayBlockingQueue<RequestData>(capacity, true);
		this.getQueue = new ArrayBlockingQueue<RequestData>(getCapacity);
	}
	
	public void setAsync(AsyncClient asyncClient){
		this.asyncCLient = asyncClient;
	}
	
	public AsyncClient getAsync(){
		return this.asyncCLient;
	}

}
