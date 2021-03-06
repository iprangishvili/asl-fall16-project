import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;


public class ConsistentHash {
	
	 private MessageDigest hashFunction;
	 private int numberOfReplicas;
	 private final SortedMap<BigInteger, String> circle = new TreeMap<BigInteger, String>();
	 private LinkedList<String> serverIPs;
	 
//	 private static final BigInteger ONE;
	
	 /**
	  * creates hash for each server and virtual node
	  * @param numberOfReplicas - number of virtual nodes for each server (for uniform distribition of hash key)
	  * @param nodes - collection of server addresses
	  * @throws NoSuchAlgorithmException
	  */
	 public ConsistentHash(int numberOfReplicas, Collection<String> nodes) {
	
	   // using MD5 hashing
	   try {
		this.hashFunction = MessageDigest.getInstance("MD5");
		
		this.numberOfReplicas = numberOfReplicas;
		
//		add servers to ring without virtual nodes
		   for (String node : nodes) {
		     add(node);
		   }
		   
//		create array list of server IPs based on the order in which they are in the ring
		SortedMap<BigInteger, String> tailmap = circle.tailMap(circle.firstKey());
		serverIPs = new LinkedList<String>();
		Iterator<String> tailIterator = tailmap.values().iterator();
		String curr_server;
	     while(tailIterator.hasNext()){
	    	 curr_server = tailIterator.next();
	    	 serverIPs.add(new String(curr_server));
	     }	 
	     
//		add virtual nodes
		   for (String node : nodes) {
		     addReplica(node);
		   }
		   
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 
	 }
	
	 /**
	  * add a specific server IP:PORT hash to the ring
	  * with specified number of virtual nodes
	  * @param node - Key to hash
	  */
	 private void addReplica(String node) {
	   for(int i=1; i<numberOfReplicas; i++){
		   byte[] messageDigest = hashFunction.digest((node + "#" + i).getBytes());
		   BigInteger number = new BigInteger(1, messageDigest);
//		   System.out.println("string: " + node + " hash: " + number);
		   circle.put(number, node);   
	   }
	 }
	 
	 /**
	  * add a specific server IP:PORT hash to the ring
	  * @param node
	  */
	 private void add(String node){
		 byte[] messageDigest = hashFunction.digest((node).getBytes());
		 BigInteger number = new BigInteger(1, messageDigest);
//		 System.out.println("string: " + node + " hash: " + number);
		 circle.put(number, node);
	 }
	
//	 private void remove(String node) {
//	   for (int i = 0; i < numberOfReplicas; i++) {
//	     circle.remove(hashFunction.hash(node.toString() + i));
//	   }
//	 }
//	
	 /**
	  * get a value of the closest hash key on the circle to the specified input String
	  * @param key - key to hash
	  * @return the value associated with the closest key to the input String
	  */
	 public String get(String key) {
	   if (circle.isEmpty()) {
	     return null;
	   }
	   
	   // hash the input key
	   byte[] messageDigest = hashFunction.digest(key.getBytes());
       BigInteger number = new BigInteger(1, messageDigest);
       
       // find the closest key on the circle (clockwise) to the new hash(input key)
	   if (!circle.containsKey(number)) {
	     SortedMap<BigInteger, String> tailMap = circle.tailMap(number);
	     number = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
	   }
	   return circle.get(number);
	 }
	 
	 /**
	  * get a list of values of hash keys on the circle closest to the
	  * specified input string (length of the output list is number of replication)
	  * @param key
	  * @param numReplica
	  * @return List of hash keys for servers 
	  */
//	 public ArrayList<String> getWithReplica(String key, int numReplica){
//		 if(circle.isEmpty()){
//			 return null;
//		 }
//		 else{
//			 // hash the input key
//			 byte[] messageDigest = hashFunction.digest(key.getBytes());
//			 BigInteger number = new BigInteger(1, messageDigest);
//			 
//			 Map<String, String> replicaServers = new LinkedHashMap<String, String>();
//			 ArrayList<String> servers = new ArrayList<String>();
//	    	 SortedMap<BigInteger, String> tailmap;
//	    	 Iterator<String> tailIterator;
//			 String curr_server;
//
//		     while(numReplica > 0){
//		    	 tailmap = circle.tailMap(number);
//		    	 if(tailmap.isEmpty()){
//		    		 tailmap = circle.tailMap(circle.firstKey());
//		    	 }
//			     tailIterator = tailmap.values().iterator();
//
//			     while(tailIterator.hasNext()){
//			    	 curr_server = tailIterator.next();
//			    	 if(replicaServers.get(curr_server) == null){
//			    		 servers.add(curr_server);
//			    		 replicaServers.put(curr_server, curr_server);
//			    		 numReplica--;
//			    	 }
//			    	 if(numReplica == 0){
//			    		 break;
//			    	 }
//			     }
//			     number = circle.firstKey();
//		     }
//		     return servers;
//		 }
//	 }
	 
	 /**
	  * get a list of values of hash keys on the circle closest to the
	  * specified input string (length of the output list is number of replication)
	  * @param key
	  * @param numReplica
	  * @return List of hash keys for servers 
	  */
	 public ArrayList<String> getWithReplica(String key, int numReplica){
		 if (circle.isEmpty()) {
		     return null;
		   }
		   
	   // hash the input key
	   byte[] messageDigest = hashFunction.digest(key.getBytes());
       BigInteger number = new BigInteger(1, messageDigest);
       
       // find the closest key on the circle (clockwise) to the new hash(input key)
	   if (!circle.containsKey(number)) {
	     SortedMap<BigInteger, String> tailMap = circle.tailMap(number);
	     number = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
	   }
	   
	   ArrayList<String> res_servers = new ArrayList<String>();
	   res_servers.add(circle.get(number)); // add primary server IP to list
//	   System.out.println("primary: " + res_servers);
	   numReplica--;
	   
//	   search in serverIPs array for the selected server and get neighboring servers for replication
	   boolean foundServer = false;
	   for(int i=0; i<serverIPs.size(); i++){
		   // found the primary server
		   if(serverIPs.get(i).equals(res_servers.get(0))){
//			   System.out.println("found server: " + i);
			   foundServer = true;
		   }
		   else if(foundServer){
			   res_servers.add(serverIPs.get(i));
//			   System.out.println("added: "  + serverIPs.get(i));
			   numReplica--;
		   }
		   if(numReplica == 0){
			   break;
		   }
		   // if numreplication is not satisfied and loop is to end 
		   // reset index to zero
		   if(numReplica != 0 && i == serverIPs.size() -1){
			   i = -1;
		   }
	   }
	   return res_servers;
	 }
	 
	 

}
