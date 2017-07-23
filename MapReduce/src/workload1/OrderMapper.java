package workload1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<Object, Text, Text, Text> 
{
	private Text keyOut = new Text(), valueOut = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] dataArray = value.toString().split("\t");
		System.out.println("Array size of splited input data: " + dataArray.length);

		String photonum = dataArray[1].trim();
		String placeName = dataArray[0].trim();
		String tagsFreq = "";
		
		keyOut.set(photonum);
		
		for (int i = 2; i < dataArray.length; i++)
		{
			tagsFreq += dataArray[i].trim() + "\t";
		}
		
		valueOut.set(placeName + "\t" + tagsFreq.trim());
		context.write(keyOut, valueOut);	
	}

}