package workload2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();

	public static Map.Entry[] sortByOrder(Map h) {  
		Set set = h.entrySet(); 
		Map.Entry[] entries = (Map.Entry[]) set.toArray(new Map.Entry[set.size()]);  
		Arrays.sort(entries, new Comparator() {  
            public int compare(Object arg0, Object arg1) {  
                Long key1 = Long.valueOf(((Map.Entry) arg0).getValue().toString());  
                Long key2 = Long.valueOf(((Map.Entry) arg1).getValue().toString());  
               	return key2.compareTo(key1); 
               } }); 
        return entries;  
    }
	
	public void reduce(Text key, Iterable<Text> values, Context context
			) throws IOException, InterruptedException {
		Map<String, Integer> Toplocality = new HashMap<String,Integer>();
		Map<String, String> Topneighbour = new HashMap<String,String>();
		Map<String, String> localityOwner = new HashMap<String,String>();
		Map<String, Integer> localityOwnerNum = new HashMap<String,Integer>();
		Map<String, String> localityNeighbour = new HashMap<String,String>();
		Map<String, String> neighbourOwner = new HashMap<String,String>();
		Map<String, Integer> neighbourOwnerNum = new HashMap<String,Integer>();
		//Map<String, String> Neighbour = new HashMap<String,String>();
		
		for (Text text: values){
			String[] info = text.toString().split("\t");
			String locality = info[0];
			String neighbour = info[1];
			String owner = info[2];
			int maxneighbour = 0;
			
			if ( ! localityOwner.containsKey(locality)){ 
				localityOwner.put(locality, owner);
				if ( localityOwner.containsValue(owner)){ 
					if ( localityOwnerNum.containsKey(owner)){
						localityOwnerNum.put(owner, localityOwnerNum.get(owner) + 1);
					} else{
						localityOwnerNum.put(owner, 1);
					}
					Toplocality.put(locality, localityOwnerNum.size());	
				}
			}
				
			if ( ! localityNeighbour.containsKey(locality) && ! neighbour.equals("NULL") ){
				localityNeighbour.put(locality, neighbour);
				if ( localityNeighbour.containsValue(neighbour)){
						neighbourOwner.put(neighbour, owner);
					if ( neighbourOwnerNum.containsKey(owner)){
						neighbourOwnerNum.put(owner, neighbourOwnerNum.get(owner) + 1);
					} else{
						neighbourOwnerNum.put(owner, 1);
					}
					if (neighbourOwnerNum.size() > maxneighbour);
					{
						maxneighbour = neighbourOwnerNum.size();
						String neighbourinfo = neighbour + ":" + maxneighbour;
						Topneighbour.put(locality, neighbourinfo);
					}
					//String v = neighbour + ":" + Topneighbour.get(neighbour);
					//Neighbour.put(locality, v);
				}
			}
		}
		
		StringBuffer strBuf = new StringBuffer();
		Map.Entry<String, Integer>[] localitytag10 = sortByOrder(Toplocality);
		for(int i = 0; i<10 && i<localitytag10.length ;i++){
			strBuf.append("{" + localitytag10[i].getKey() + ":" + localitytag10[i].getValue() + "," 
					 + Topneighbour.get(localitytag10[i].getKey())+ "}\t" );
		}
		result.set(strBuf.toString());
		context.write(key, result);
		
	}
}
