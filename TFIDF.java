/**
 * Name: Sampath Sree Kumar K
 * Email-id: skolluru@uncc.edu
 * Studentid: 800887568
 */
package org.myorg;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileStatus;
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
 * 
 */

public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);

	private static final String TF_Map_Output = "TF_Map_Output";

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TFIDF(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		FileSystem fs = FileSystem.get(getConf());

		Path InputFilepath = new Path(args[0]); // Get input file path

		// Remove final output path
		Path OutputPath = new Path(args[1]);
		if (fs.exists(OutputPath)) {
			fs.delete(OutputPath, true);
		}

		// Remove the term frequency output path
		Path TermFreqPath = new Path(TF_Map_Output);
		if (fs.exists(TermFreqPath)) {
			fs.delete(TermFreqPath, true);
		}

		// Get the number files present in the input using system commands
		FileStatus[] FilesList = fs.listStatus(InputFilepath);
		final int totalinputfiles = FilesList.length;

		// Execute Term Frequency for given input
		Job job1 = new Job(getConf(), "TermFrequency");
		job1.setJarByClass(this.getClass());
		job1.setMapperClass(TF_Map.class);
		job1.setReducerClass(TF_Reduce.class);
		// map output types
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		// reducer output types - Value class as DoubleWritable
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job1, InputFilepath);
		FileOutputFormat.setOutputPath(job1, TermFreqPath);

		job1.waitForCompletion(true);

		// Execute TFIDF program using the previous job's output as input
		Job job2 = new Job(getConf(), "CalculateTFIDF");
		job2.getConfiguration().setInt("totalinputfiles", totalinputfiles);
		job2.setJarByClass(this.getClass());
		job2.setMapperClass(TF_IDF_Map.class);
		job2.setReducerClass(TF_IDF_Reduce.class);
		// map output types
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		// reduce output types
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job2, TermFreqPath);
		FileOutputFormat.setOutputPath(job2, OutputPath);

		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static class TF_Map extends
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

	public static class TF_Reduce extends
			Reducer<Text, IntWritable, Text, DoubleWritable> {
		// @Override
		public void reduce(Text word, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			// Calculate Term Frequency in logarithmic form
			double tf = Math.log10(10) + Math.log10(sum);
			// Change the return type to DoubleWritable to return double value
			context.write(word, new DoubleWritable(tf));
		}
	}

	public static class TF_IDF_Map extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text word_key = new Text();
		private Text filename_tf = new Text();

		/*
		 * Input to map is in the form <Hadoop#####file3 1.3010299956639813>
		 * <Oh#####file2 1.0>
		 */

		public void map(LongWritable Key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] word_file_tf = value.toString().split("\t");
			String[] word_file = word_file_tf[0].toString().split("#####");
			this.word_key.set(word_file[0]);
			this.filename_tf.set(word_file[1] + "=" + word_file_tf[1]);
			context.write(word_key, filename_tf);
		}
	}

	public static class TF_IDF_Reduce extends
			Reducer<Text, Text, Text, DoubleWritable> {

		private Text word_file_key = new Text();
		private double tfidf;

		/*
		 * Input to reduce phase is in the form <"Hadoop",
		 * ["file1.txt=1.3010299956639813","file2.txt=1.0"]> <"is",
		 * ["file1.txt=1.0", "file2.txt=1.0"]>
		 */

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double docswithword = 0;
			Map<String, Double> tempvalues = new HashMap<String, Double>();
			for (Text v : values) {
				String[] filecounter = v.toString().split("=");
				docswithword++;	//Calculate the number of files in which the term appeared
				tempvalues.put(filecounter[0], Double.valueOf(filecounter[1]));
			}

			// get the number of documents in corpus
			int numoffiles = context.getConfiguration().getInt(
					"totalinputfiles", 0);
			//Calculate IDF values
			double idf = Math.log10(numoffiles / docswithword);
			for (String temp_tfidf_file : tempvalues.keySet()) {
				this.word_file_key.set(key.toString() + "#####"
						+ temp_tfidf_file);
				//Calculate TF-IDF values and write them to output
				this.tfidf = tempvalues.get(temp_tfidf_file) * idf;
				context.write(this.word_file_key,
						new DoubleWritable(this.tfidf));
			}
		}
	}

}
