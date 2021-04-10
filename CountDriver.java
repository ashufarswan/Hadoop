
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException 
	{
		Configuration conf = new Configuration();
		Job j = new Job();
		j.setJobName("Count");
		j.setJarByClass(CountDriver.class );
		j.setMapperClass(CountMapper.class );
		j.setReducerClass(CountReducer.class);
		
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		
		URI uri = new URI(args[1].toString());
		
		FileSystem fs =  FileSystem.get(uri, conf);
		
		boolean x = fs.delete(new Path(uri),true);
		
		
		int xxx =  j.waitForCompletion(true) ? 0 : 1;
		
	}

}




