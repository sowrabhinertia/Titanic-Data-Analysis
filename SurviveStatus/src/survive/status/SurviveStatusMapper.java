package survive.status;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SurviveStatusMapper extends Mapper<LongWritable,Text,Text,IntWritable>
{
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		Text people = new Text();
		String [] tokens = value.toString().split(",");
		StringBuffer str = new StringBuffer();
		if(tokens.length==12)
		{
			str.append(tokens[1]);
			str.append(" ");
			str.append(tokens[4]);
			str.append(" ");
			str.append(tokens[5]);
			context.write(new Text(str.toString()), new IntWritable(1));
		}
	}
}
