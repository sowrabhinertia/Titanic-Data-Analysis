package aveeage.age;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgAgeReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>
{
	public void reduce(Text key,Iterable<FloatWritable> values,Context context) throws IOException,InterruptedException
	{
		float sum = 0;
		int count = 0;
		for(FloatWritable val:values)
		{
			sum = sum+val.get();
			count++;
		}
		float avg = sum/count;
		context.write(key,new FloatWritable(avg));
		
	}
}
