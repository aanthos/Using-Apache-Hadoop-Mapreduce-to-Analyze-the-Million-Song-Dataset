package cs455.hadoop.Q1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Q3. What is the song with the highest hotttnesss (popularity) score?
 * Grabs from Analysis file?
 */
public class AnalysisFileMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Tokenize metadata file into tokens delimited by commas. 
		String[] tokens = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		
		// Question 3
		String song_id = tokens[1];
		String song_hotness = tokens[2];
		String loudness = tokens[10];
		String end_of_fade_in = tokens[6];
		String duration = tokens[5];
		String danceability = tokens[4];
		String energy = tokens[7];
		
		// Emit song_id, song_hotness, track_id
		context.write(new Text(song_id), new Text("Analysis" + "," + song_id + "," + song_hotness + "," + loudness + "," + end_of_fade_in + "," + duration + "," + danceability + "," + energy));
	}
}
