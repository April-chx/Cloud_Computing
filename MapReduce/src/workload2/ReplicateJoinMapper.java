package workload2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ReplicateJoinMapper extends Mapper<Object, Text, Text, Text> {
	private Hashtable <String, String> countryTable = new Hashtable<String, String>();
	private Hashtable <String, String> localityTable = new Hashtable<String, String>();
	private Hashtable <String, String> neighbourTable = new Hashtable<String, String>();
	private Text keyOut = new Text(), valueOut = new Text();
	
	public void setCountryTable(Hashtable<String,String> country){
		countryTable = country;
	}
	public void setLocalityTable(Hashtable<String,String> locality){
		localityTable = locality;
	}
	public void setNeighbourTable(Hashtable<String,String> neighbour){
		neighbourTable = neighbour;
	}
	
	// get the distributed file and parse it
	public void setup(Context context)
		throws java.io.IOException, InterruptedException{
		
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (cacheFiles != null && cacheFiles.length > 0) {
			String line;
			String[] tokens;
			BufferedReader placeReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			try {
				while ((line = placeReader.readLine()) != null) {
					tokens = line.split("\t");
					countryTable.put(tokens[0], tokens[1]);  // use full place.txt index is 6, other wise it is 1.
					localityTable.put(tokens[0], tokens[2]);
					neighbourTable.put(tokens[0], tokens[3]);
				}
				//System.out.println("size of the place table is: " + placeTable.size());
			} 
			finally {
				placeReader.close();
			}
		}
	}
	
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); //split the data into array
		if (dataArray.length < 5){ // a not complete record with all data
			return; // don't emit anything
		}
		
		String placeId = dataArray[4];
		String countryname = countryTable.get(placeId);
		String localityname = localityTable.get(placeId);
		String neighbourname = neighbourTable.get(placeId);
		
		if (countryTable.containsKey(placeId)){ 
			keyOut.set(countryname);
			valueOut.set(localityname + "\t" + neighbourname + "\t" + dataArray[1]);
			context.write(keyOut, valueOut);
		}

	}

}
