package cs455.hadoop.Q1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author anthos
 * 
 * References:
 * https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
 * http://codingjunkie.net/text-processing-with-mapreduce-part1/
 * 
 * Q1 Mapper: Reads line by line, 
 * Grabs from Metadata file?
 */
public class Q1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Tokenize metadata file into tokens delimited by commas.
		String[] tokens = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		
		// Question 1
		String artist_id = tokens[3];
		String artist_name = tokens[7];
		String song_id = tokens[8];
		String title = tokens[9];
	
		// emit artist_id, count pairs to Reducer
		// TODO Add "Metadata" string to tell Reducer what file to process?
		context.write(new Text(artist_id), new Text("Metadata" + "," + artist_name + "," + song_id + "," + title + "," + "1"));
	}
}

/*
 * Regex used above but friendlier for the eyes
 */
// String otherThanQuote = " [^\"] ";
// String quotedString = String.format(" \" %s* \" ", otherThanQuote);
// String regex = String.format("(?x) "+ // enable comments, ignore white spaces
//            ",                         "+ // match a comma
//            "(?=                       "+ // start positive look ahead
//            "  (?:                     "+ //   start non-capturing group 1
//            "    %s*                   "+ //     match 'otherThanQuote' zero or more times
//            "    %s                    "+ //     match 'quotedString'
//            "  )*                      "+ //   end group 1 and repeat it zero or more times
//            "  %s*                     "+ //   match 'otherThanQuote'
//            "  $                       "+ // match the end of the string
//            ")                         ", // stop positive look ahead
//            otherThanQuote, quotedString, otherThanQuote);
//
//    String[] tokens = line.split(regex, -1);