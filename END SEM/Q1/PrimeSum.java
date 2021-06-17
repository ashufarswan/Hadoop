import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SumPrime
{
	public static void main(String[] args) throws Exception 
	{
		// creating object of configuration class 
		Configuration conf = new Configuration();
				
		Job j = new Job(conf, "Sum of Prime Number");
		// setting name of main class
		j.setJarByClass(SumPrime.class);
		//name of mapper class
		j.setMapperClass(SP_Mapper.class);
		// name of reducer class
		j.setReducerClass(SP_Reducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));

		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class SP_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		 private boolean CheckPrime(int numberToCheck) {
        		int remainder;
        		for (int i = 2; i <= numberToCheck / 2; i++) {
            			remainder = numberToCheck % i;
            			if (remainder == 0) {
                			return false;
            			}
        		}
        		return true;
		   }
		public void map(LongWritable key, Text value, Context c)throws IOException, InterruptedException
		{
			String data[]=value.toString().split(",");
			for(String num:data)
			{
				int number=Integer.parseInt(num);
				if(CheckPrime(number))
				{
				c.write(new Text("PRIME"), new IntWritable(number));   
				}
				else
				{
				c.write(new Text("Composite"), new IntWritable(number));   
				}
			}					
		}
	}

	public static class SP_Reducer extends	Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text word, Iterable<IntWritable> value_list, Context c)throws IOException, InterruptedException
		{
			
			int sum = 0;
			String s = word.toString();
			String s1 = "PRIME";
			if(s.equals(s1)){
				for (IntWritable new_value : value_list) 
				{
				sum =sum+ new_value.get();
				}
				//output
				c.write(word, new IntWritable(sum));
			}
		}
	}
}
