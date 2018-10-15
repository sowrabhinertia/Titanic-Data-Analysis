package aveeage.age;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageAgeMapper extends Mapper<LongWritable,Text,Text,FloatWritable>
{
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		String tokens[] = value.toString().split(",");
		if(tokens.length==12)
		{
			if(tokens[5].length()>=1)
			{
				float intval = Float.parseFloat(tokens[5].toString());

				context.write(new Text(tokens[4]), new FloatWritable(intval));
			}
		}
	}
}
