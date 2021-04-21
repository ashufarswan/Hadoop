
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int y = 0;
		for(IntWritable x : values)
		{
			y ++;
			
		}
		if(key.equals("Apple")||key.equals("Banana")||key.equals("Grapes")){
			context.write(new Text(key),new IntWritable(y));
		}
		
		
	}

}

