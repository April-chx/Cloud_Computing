package workload1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PlaceFilterMapper extends Mapper<Object, Text, Text, Text> {
	private Text placeId= new Text(), placeLocality = new Text();
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); 
		if (dataArray.length < 7){ 
			return;
		}
		
		if (dataArray[5].equals("7")){
		
			String[] placeUrl = dataArray[6].split("/");
			placeId.set(dataArray[0]);
			placeLocality.set(placeUrl[placeUrl.length-1]);
			context.write(placeId, placeLocality);
		}
		else if (dataArray[5].equals("22"))
		{
			String[] placeUrl = dataArray[6].split("/");
			placeId.set(dataArray[0]);
			placeLocality.set(placeUrl[placeUrl.length-2]);
			context.write(placeId, placeLocality);
		}
	}
}
