/**
 * Name: Sampath Sree Kumar K
 * Email-id: skolluru@uncc.edu
 * Studentid: 800887568
 */
package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import java.lang.Math;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

/**
 * 
 * @author sam
 *
 */


public class TermFrequency extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TermFrequency.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TermFrequency(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		FileSystem fs = FileSystem.get(getConf());

		Path InputFilepath = new Path(args[0]);	//Get input file path

		// Remove final output path
		Path OutputPath = new Path(args[1]);
		if (fs.exists(OutputPath)) {
			fs.delete(OutputPath, true);
		}
		//Create a job
		Job job = new Job(getConf(), "TermFreq");
		job.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is
		// used
		FileInputFormat.addInputPath(job, InputFilepath);
		FileOutputFormat.setOutputPath(job, OutputPath);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// map output types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// reducer output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			String fileName = ((FileSplit) context.getInputSplit()).getPath()
					.getName();

			Text currentWord = new Text();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}

				currentWord = new Text(word + "#####" + fileName);
				context.write(currentWord, one);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, DoubleWritable> {
		// @Override
		public void reduce(Text word, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			//Calculate Term Frequency in logarithmic form
			double tf = Math.log10(10) + Math.log10(sum);
			//Change the return type to DoubleWritable to return double value
			context.write(word, new DoubleWritable(tf));
		}
	}
}
