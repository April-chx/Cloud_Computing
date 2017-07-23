package workload1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ReplicateJoinMapper extends Mapper<Object, Text, Text, Text> {
	private Hashtable <String, String> placeTable = new Hashtable<String, String>();
	private Text keyOut = new Text(), valueOut = new Text();
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); 
	
	public void setPlaceTable(Hashtable<String,String> place){
		placeTable = place;
	}
	
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
					placeTable.put(tokens[0], tokens[1]); 
				}
			} 
			finally {
				placeReader.close();
			}
		}
	}
	@Override
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); 
		if (dataArray.length < 5){
			return;
		}
		String placeId = dataArray[4];
		String placeLocality = placeTable.get(placeId);
		
		if (dataArray[2].length() > 0){
			String[] tagArray = dataArray[2].split(" ");
			for(String tag: tagArray) {
				if (asciiEncoder.canEncode(tag)&&placeLocality !=null){
					keyOut.set(placeLocality);
					valueOut.set(tag + "\t" + dataArray[0]);
					context.write(keyOut, valueOut);
				}
			}
		}
	}
}