import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileInputFormat;
//import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException 
	{
		Configuration conf = new Configuration();
		//FileSystem fs = FileSystem.get(conf);
		//System.out.println(fs.getUri());
		Job j = new Job();// getConf(), "Max temperature");
		j.setJobName("My First Job");
		j.setJarByClass(WordCountDriver.class );
		j.setMapperClass(WordCountMapper.class );
		//j.setCombinerClass(WordCountReducer.class);
		//j.setPartitionerClass(WordCountPartitioner.class);
		j.setReducerClass(WordCountReducer.class);
		//j.setNumReduceTasks(0);
		//j.setCombinerClass(WordCountReducer.class);
		//j.setCombinerClass(WordCountReducer.class);
		//
		//j.setNumReduceTasks(0);
		
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
