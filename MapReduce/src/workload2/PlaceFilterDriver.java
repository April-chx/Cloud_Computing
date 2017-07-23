package workload2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class PlaceFilterDriver {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: PlaceFilter <in> <out>");
			System.exit(2);
		}
		if (otherArgs.length == 3){
			conf.set("mapper.placeFilter.country", otherArgs[2]);
		}
		Job job = new Job(conf, "Place Filter");
		job.setJarByClass(PlaceFilterDriver.class);
		job.setNumReduceTasks(0);
		job.setMapperClass(PlaceFilterMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
