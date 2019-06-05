package cs455.hadoop.Q1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author anthos
 * Q1. Which artist has the most songs in the data set?
 * Q2. Which artist’s songs are the loudest on average?
 * Q3. What is the song with the highest hotttnesss (popularity) score?
 * Q4. Which artist has the highest total time spent fading in their songs?
 * Q5. What is the longest song(s)? The shortest song(s)? The song(s) of median length?
 * Q6. What are the 10 most energetic and danceable songs? List them in descending order.
 * 
 * Brainstorming:
 * Make output file containing data for both files
 * 
 * References:
 * http://hadoop.apache.org/docs/r3.1.2/api/org/apache/hadoop/mapreduce/lib/output/MultipleOutputs.html
 * http://dailyhadoopsoup.blogspot.com/2014/01/mutiple-input-files-in-mapreduce-easy.html
 * 
 */

public class MainJob {
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();

			// Give MapRed job a name. You'll see this name in Yarn webapp
			Job job = Job.getInstance(conf, "Hi Eli");
			
			// Current class
			job.setJarByClass(MainJob.class);
			
			Path metadataFile = new Path(args[0]);
			Path analysisFile = new Path(args[1]);
			
			// Mapper
			//job.setMapperClass(Q1Mapper.class);
			MultipleInputs.addInputPath(job, metadataFile, TextInputFormat.class, MetadataFileMapper.class);
			MultipleInputs.addInputPath(job, analysisFile, TextInputFormat.class, AnalysisFileMapper.class);
			
			// Outputs from the Mapper.
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			// Reducer
			job.setReducerClass(Q1Reducer.class);
			
			// Outputs from Reducer.
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// path to input in HDFS
//			FileInputFormat.addInputPath(job, new Path(args[0]));

			// path to output in HDFS
			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			// Block until the job is completed.
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		} catch (IOException e) {
			System.err.println(e.getMessage());
		} catch (InterruptedException e) {
			System.err.println(e.getMessage());
		} catch (ClassNotFoundException e) {
			System.err.println(e.getMessage());
		}

//		try {
//			Configuration conf2 = new Configuration();
//			// Give MapRed job a name. You'll see this name in Yarn webapp
//			Job job3 = Job.getInstance(conf2, "Question 2");
//
//			// Current class
//			job3.setJarByClass(FirstJob.class);
//			// Mapper
//			job3.setMapperClass(Q3Mapper.class);
//			// Reducer
//			job3.setReducerClass(Q3Reducer.class);
//			
//			// Outputs from the Mapper
//			job3.setMapOutputKeyClass(Text.class);
//			job3.setMapOutputValueClass(Text.class);
//			
//			// Outputs from the Reducer
//			job3.setOutputKeyClass(Text.class);
//			job3.setOutputValueClass(Text.class);
//			
//			// path to input in HDFS
//			FileInputFormat.addInputPath(job3, new Path(args[0]));
//
//			// path to output in HDFS
//			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
//			
//			// Block until the job is completed.
//			System.exit(job3.waitForCompletion(true) ? 0 : 1);
//			
//		} catch (IOException e) {
//			System.err.println(e.getMessage());
//		} catch (InterruptedException e) {
//			System.err.println(e.getMessage());
//		} catch (ClassNotFoundException e) {
//			System.err.println(e.getMessage());
//		}
	}
}
