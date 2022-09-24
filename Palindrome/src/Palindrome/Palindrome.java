package Palindrome;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Palindrome
{
	public static class PallindromMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
	    private Text pallindromWord = new Text();
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException
	    {
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens())
	      {
	        String word = itr.nextToken().trim();
	        if(word.equals(inversedWord(word)))
	        {
	        	pallindromWord.set(word);
	        	context.write(pallindromWord,new IntWritable(1));
	        }
	       
	      }
	    }
		private String inversedWord(String word)
		{
			String inversedWord = "";
			for(int i = word.length()-1; i >= 0; i--)
			{
				inversedWord = inversedWord + word.charAt(i);
			}
			return inversedWord.trim();
		}	
	}
	
	public static class PallindromReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key ,Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);

		}
	}
	
	public static void main(String[] args) throws IOException,InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"wordCountPallindrom");
		job.setJarByClass(Palindrome.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(PallindromMapper.class);
		job.setCombinerClass(PallindromReducer.class);
		job.setReducerClass(PallindromReducer.class);
		 
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

	}
}
