package workload1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<Text, Text, Text, Text> 
{
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	private int locality50 = 0;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		if (locality50 < 50)
		{
			String photoFreq = key.toString();
			String tagsList = "";
			String place_name = "";
			
			for (Text text: values)
			{
				String placeAndTags = text.toString();

				tagsList = placeAndTags.substring(placeAndTags.indexOf("\t") + 1);
				place_name = placeAndTags.substring(0, placeAndTags.indexOf("\t"));
			}
			keyOut.set(place_name.trim());
			valueOut.set(photoFreq.trim() + "\t" + tagsList.trim());
			context.write(keyOut, valueOut);
			locality50++;
		}	
	}
}
