package cs455.hadoop.Q1;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q3Reducer extends Reducer<Text, Text, Text, DoubleWritable>{
	// Unsorted map that will hold song names and hotness scores
	private Map<String, Double> map;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		map = new HashMap<String, Double>();
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		// Text val contains (track_id + "," _ song_hotness) passed in from the mapper
		for(Text val : value) {
			String[] data = val.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			String track_id = data[0];
			double song_hotness = Double.parseDouble(data[1]);
			map.put(track_id, song_hotness);
		}
	}
	
	@Override
	protected void cleanup (Context context) throws IOException, InterruptedException {
		// Create list from elements of map
		List<Map.Entry<String, Double> > list = new LinkedList<Map.Entry<String, Double> >(map.entrySet());
		// Sort the list from highest to lowest. Switch o2 compareTo o1 to get lowest to highest
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
			public int compare(Map.Entry<String, Double> o1,
							   Map.Entry<String, Double> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});
		
		// Put data from sorted list to sorted hashmap by values
		HashMap<String, Double> sortedMap = new LinkedHashMap<String, Double>();
		for (Map.Entry<String, Double> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
	
		int count = 0;
		for(Map.Entry<String, Double> entry : sortedMap.entrySet()) {
			if(count >= 10) break;
			
			context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
			count++;
		}
	}
}
