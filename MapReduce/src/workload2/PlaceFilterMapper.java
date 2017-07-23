package workload2;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PlaceFilterMapper extends Mapper<Object, Text, Text, Text> {
	private Text keyOut= new Text(), valueOut = new Text();
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); //split the data into array
		if (dataArray.length < 7){ // a not complete record with all data
			return; // don't emit anything
		}
		String place = dataArray[6]; 
		String[] placeArray = place.toString().split("/");
		String localityName, neighbourName;
		int localityindex, neighbourindex;
		
		if (dataArray[5].equals("7")){
			localityindex = placeArray.length - 1;
			localityName = placeArray[localityindex]; 
			keyOut.set(dataArray[0]);
			valueOut.set(placeArray[1] + "\t" + localityName + "\t" + "NULL");
			context.write(keyOut, valueOut);
		}
		else if (dataArray[5].equals("22")){
			localityindex = placeArray.length - 2;
			neighbourindex = placeArray.length - 1;
			localityName = placeArray[localityindex];
			neighbourName = placeArray[neighbourindex];
			keyOut.set(dataArray[0]);
			valueOut.set(placeArray[1] + "\t" + localityName + "\t" + neighbourName);
			context.write(keyOut, valueOut);
		}
		
	}

}
