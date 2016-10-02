import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;


public class ConsistentHash {
	
	 private final MessageDigest hashFunction;
	 private final int numberOfReplicas;
	 private final SortedMap<BigInteger, String> circle = new TreeMap<BigInteger, String>();
	
	 public ConsistentHash(int numberOfReplicas,
	     Collection<String> nodes) throws NoSuchAlgorithmException {
		 
	   this.hashFunction = MessageDigest.getInstance("MD5");
	   this.numberOfReplicas = numberOfReplicas;
	
	   for (String node : nodes) {
	     add(node);
	   }
	 }
	
	 private void add(String node) {
	   for(int i=0; i<numberOfReplicas; i++){
		   byte[] messageDigest = hashFunction.digest((node + "#" + i).getBytes());
		   BigInteger number = new BigInteger(1, messageDigest);
		   System.out.println("string: " + node + " hash: " + number);
		   circle.put(number, node);   
	   }
	 }
	
//	 private void remove(String node) {
//	   for (int i = 0; i < numberOfReplicas; i++) {
//	     circle.remove(hashFunction.hash(node.toString() + i));
//	   }
//	 }
//	
	 public String get(String key) {
	   if (circle.isEmpty()) {
	     return null;
	   }
	   
	   byte[] messageDigest = hashFunction.digest(key.getBytes());
       BigInteger number = new BigInteger(1, messageDigest);
	   
	   if (!circle.containsKey(number)) {
	     SortedMap<BigInteger, String> tailMap = circle.tailMap(number);
	     number = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
	   }
	   return circle.get(number);
	 }

}
