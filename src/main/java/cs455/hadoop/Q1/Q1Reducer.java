package cs455.hadoop.Q1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * 
 * References:
 * http://timepasstechies.com/mapreduce-topn/
 * https://pivotalhd-210.docs.pivotal.io/tutorial/getting-started/map-reduce-java/summarization-postalcode-paidamount.html
 * https://www.geeksforgeeks.org/sorting-a-hashmap-according-to-values/
 * 
 * Brainstorming:
 * Fill variables, then place in appropriate maps for sorting/limiting entries
 */
public class Q1Reducer extends Reducer<Text, Text, Text, Text> {

	//private TreeMap<Integer, String> treeMap = new TreeMap<String, Integer>();

	// Quesiton 1 - Map that connects artist with amount of songs
	//private Map<String, Integer> unsortedMostSongsMap;
	
	// Question 3

	// Mini-lookup map from Analysis and Metadata file with song_id as the key (Used for: Question 3).
	// Contains information relevant to each song_id from both files such as songHotness in Analysis and songTitle in Metadata.
	
	// Analysis File Lookup Maps
	private Map<String, Double> songIDHotnessMap;
	private Map<String, Double> songIDLoudnessMap;
	private Map<String, Double> songIDFadeInMap;
	private Map<String, Double> songIDDurationMap;
	private Map<String, double[]> songIDDanceabilityEnergyMap;
	
	// Metadata File Lookup Maps
	private Map<String, Integer> artistIDSongCountMap;
	private Map<String, Set<String>> artistSongIDsMap;
	private Map<String, Set<String>> artistIDNameMap;
	//private Map<String, String> songIDartistIDMap;
	private Map<String, String> songIDTitleMap;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		artistIDSongCountMap = new HashMap<String, Integer>();
		artistSongIDsMap = new HashMap<String, Set<String>>();
		artistIDNameMap = new HashMap<String, Set<String>>();
		//unsortedMostSongsMap = new HashMap<String, Integer>();
		songIDTitleMap = new HashMap<String, String>();
		songIDHotnessMap = new HashMap<String, Double>();
		songIDLoudnessMap = new HashMap<String, Double>();
		//songIDartistIDMap = new HashMap<String, String>();
		songIDFadeInMap = new HashMap<String, Double>();
		songIDDurationMap = new HashMap<String, Double>();
		songIDDanceabilityEnergyMap = new HashMap<String, double[]>();
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		/*
		 * Variable Initializing Section
		 * TODO Sort variables into analysis and metadata
		 */
		// Identifier
		String identifier = "";
		
		int songCount = 0;
		String artistName = "";
		Set<String> artistNames = new HashSet<String>();
		Set<String> artistSongs = new HashSet<String>();
		
		String artistID = key.toString();
		double songLoudness = 0.0;
		
		String songIDMetadata = "";
		String songIDAnalysis = "";
		String songTitle = "";
		double songHotness = 0.0;
		double fadeInTime = 0.0;
		double songDuration = 0.0;
		
		double danceability = 0.0;
		double energy = 0.0;
		double[] danceEnergyContainer = new double[2];
		/*
		 * Variable Setting Section
		 * Goes through each value emitted from map for the same key and then resets everything
		 * for the next unique key
		 */
		for(Text val : value) {
			String[] data = val.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			identifier = data[0];
			if(identifier.equals("Metadata")) {
				artistName = data[1];
				artistNames.add(artistName);
				songIDMetadata = data[2];
				artistSongs.add(songIDMetadata);
				songTitle = data[3];
				songCount += Integer.parseInt(data[4]);
			}

			if(identifier.equals("Analysis")) {
				songIDAnalysis = data[1];
				
				// checks if double
				try { songHotness = Double.parseDouble(data[2].trim()); }
				catch(NumberFormatException e) {}
				// checks if double
				try { songLoudness = Double.parseDouble(data[3].trim()); }
				catch(NumberFormatException e) {}
				
				// checks if double
				try { fadeInTime = Double.parseDouble(data[4].trim()); }
				catch(NumberFormatException e) {}
				
				try { songDuration = Double.parseDouble(data[5].trim()); }
				catch(NumberFormatException e) {}
				
				try { danceability = Double.parseDouble(data[6].trim()); }
				catch(NumberFormatException e) {}
				
				try { energy = Double.parseDouble(data[7].trim()); }
				catch(NumberFormatException e) {}
				
				danceEnergyContainer[0] = danceability;
				danceEnergyContainer[1] = energy;
			}
		}
		
		/*
		 * Mapper/Data Structure Section
		 */

		// Place all artist aliases who have the same artist_id together
//		String artistNames = "Artist Name(s): ";
//		if(!artistAliases.isEmpty()) {
//			for(String alias : artistAliases) {
//				artistNames += alias;
//			}
//		}
		
		
		if(identifier.equals("Metadata")) {
			// Question 1
			//unsortedMostSongsMap.put(artistNames, songCount);
			artistIDSongCountMap.put(artistID, songCount);
			artistIDNameMap.put(artistID, artistNames);
			// Question 2
			//songIDartistIDMap.put(artistID, songIDMetadata);
			artistSongIDsMap.put(artistID, artistSongs);
			// Question 3
			songIDTitleMap.put(songIDMetadata, songTitle);	
		}
		
		else if(identifier.equals("Analysis")) {
			// Question 2
			songIDLoudnessMap.put(songIDAnalysis, songLoudness);
			// Question 3
			songIDHotnessMap.put(songIDAnalysis, songHotness);
			// Quesiton 4
			songIDFadeInMap.put(songIDAnalysis, fadeInTime);
			// Question 5
			songIDDurationMap.put(songIDAnalysis, songDuration);
			// Question 6
			songIDDanceabilityEnergyMap.put(songIDAnalysis, danceEnergyContainer);
		}
	}

	@Override
	// TODO Clean up by calling different methods
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Question 1
		// Limits to only top 10
		context.write(new Text("\nQuestion 1\n"), new Text(""));
		
		HashMap<String, Integer> sortedArtistIDSongCountMap = sortMapByValue(artistIDSongCountMap);
		int count = 0;
		for(Map.Entry<String, Integer> entry : sortedArtistIDSongCountMap.entrySet()) {
			if(count >= 10) break;
			
			String artistAliases = artistIDNameMap.get(entry.getKey()).toString();
			context.write(new Text(artistAliases), new Text("" + entry.getValue()));
			count++;
		}
		
		// Question 2
		context.write(new Text("\nQuestion 2\n"), new Text(""));
		HashMap<String, Double> unsortedArtistAvgLoudness = new HashMap<String, Double>();
		for(Map.Entry<String, Set<String>> entry : artistSongIDsMap.entrySet()) {
			double numSongs = 0.0;
			double sumLoudness = 0.0;
			double average = 0.0;
			String songIDs = "";
			for(String songID : entry.getValue()) {
				songIDs += songID + ",";
			}
			
			String[] split = songIDs.substring(0, songIDs.length() - 1).split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			
			
			for(String s : split) {
				if(songIDLoudnessMap.containsKey(s)) {
					//context.write(new Text("True"), new Text(""));
					sumLoudness += songIDLoudnessMap.get(s);
					//context.write(new Text("" + songIDLoudnessMap.get(s)), new Text(""));
				}
				numSongs++;
			}
			average = sumLoudness / numSongs;
			String artistAliases = artistIDNameMap.get(entry.getKey()).toString();
			unsortedArtistAvgLoudness.put(artistAliases, average);
		}
		
		HashMap<String, Double> sortedArtistAvgLoudness = sortMapByFloatValue(unsortedArtistAvgLoudness);
		count = 0;
		for(Map.Entry<String, Double> entry : sortedArtistAvgLoudness.entrySet()) {
			if(count >= 10) break;
			
			context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
			count++;
		}

		// Question 3
		// Limits to only top 10
		context.write(new Text("\nQuestion 3\n"), new Text(""));
		HashMap<String, Double> sortedHottestSongsMap = sortMapByFloatValue(songIDHotnessMap);
		count = 0;
		for(Map.Entry<String, Double> entry : sortedHottestSongsMap.entrySet()) {
			if(count >= 10) break;
			// need this if statement since one songID might be in the analysis file but not metadata
			if(songIDTitleMap.containsKey(entry.getKey())) {
				context.write(new Text(songIDTitleMap.get(entry.getKey())), new Text("" + entry.getValue()));
				count++;
			}
//			else {
//				context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
//				count++;
//			}
		}
		
		// Question 4
		// Limits to only top 10
		context.write(new Text("\nQuestion 4\n"), new Text(""));
		
		Map<String, Integer> unsortedArtistTotalFadeInMap = new HashMap<String, Integer>();
		
		for(Map.Entry<String, Set<String>> entry : artistSongIDsMap.entrySet()) {
			String songIDs = "";
			for(String songID : entry.getValue()) {
				songIDs += songID + ",";
			}
			
			String[] split = songIDs.substring(0, songIDs.length() - 1).split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			
			int totalFadeIn = 0;
			for(String s : split) {
				
				if(songIDFadeInMap.containsKey(s)) {
					totalFadeIn += songIDFadeInMap.get(s);
//					context.write(new Text(s), new Text(""));
//					try {
//						context.write(new Text("" + songIDFadeInMap.get(s)), new Text(""));
//					}
//					catch(Exception e) {}
				}
			}
			String artistAliases = artistIDNameMap.get(entry.getKey()).toString();
			unsortedArtistTotalFadeInMap.put(artistAliases, totalFadeIn);
		}
		
		HashMap<String, Integer> sortedArtistTotalFadeInMap = sortMapByValue(unsortedArtistTotalFadeInMap);
		count = 0;
		for(Map.Entry<String, Integer> entry : sortedArtistTotalFadeInMap.entrySet()) {
			if(count >= 10) break;
			
			context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
			count++;
		}
		
		// Question 5
		context.write(new Text("\nQuestion 5\n"), new Text(""));
		
		// Top 10 max values
		context.write(new Text("Longest Songs:"), new Text(""));
		HashMap<String, Double> sortedSongIDDurationMap = sortMapByFloatValue(songIDDurationMap);
		count = 0;
		for(Map.Entry<String, Double> entry : sortedSongIDDurationMap.entrySet()) {
			if(count >= 10) break;
			
			if(songIDTitleMap.containsKey(entry.getKey())) {
				context.write(new Text(songIDTitleMap.get(entry.getKey())), new Text("" + entry.getValue()));
				count++;
			}
//			else {
//				context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
//				count++;
//			}
		}
		
		// Top 10 min values
		context.write(new Text("Shortest Songs:"), new Text(""));
		HashMap<String, Double> sortedSongIDShortestDurationMap = sortMapByFloatValueDescending(songIDDurationMap);
		count = 0;
		for(Map.Entry<String, Double> entry : sortedSongIDShortestDurationMap.entrySet()) {
			if(count >= 10) break;
			
			if(songIDTitleMap.containsKey(entry.getKey())) {
				context.write(new Text(songIDTitleMap.get(entry.getKey())), new Text("" + entry.getValue()));
				count++;
			}
//			else {
//				context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
//				count++;
//			}
		}
		
		// median value(s)
		context.write(new Text("Median Value(s):"), new Text(""));
		int size = (sortedSongIDDurationMap.size() - 1) / 2;
		
		if(sortedSongIDDurationMap.size() % 2 == 0) {
			int upperMedian = size + 1;
			int lowerMedian = size;
			int position = 0;
			for(Map.Entry<String, Double> entry : sortedSongIDDurationMap.entrySet()) {
				if(position == lowerMedian) {
					//String songName = songIDTitleMap.get(entry.getKey()).toString();
					//context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
					if(songIDTitleMap.containsKey(entry.getKey())) {
						context.write(new Text(songIDTitleMap.get(entry.getKey())), new Text("" + entry.getValue()));
					}
				}
				if(position == upperMedian) {
					//context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
					if(songIDTitleMap.containsKey(entry.getKey())) {
						context.write(new Text(songIDTitleMap.get(entry.getKey())), new Text("" + entry.getValue()));
					}
				}
				position++;
			}
		}
		else {
			int median = size;
			int position = 0;
			for(Map.Entry<String, Double> entry : sortedSongIDDurationMap.entrySet()) {
				if(position == median) {
					//context.write(new Text(entry.getKey()), new Text("" + entry.getValue()));
					if(songIDTitleMap.containsKey(entry.getKey())) {
						context.write(new Text(songIDTitleMap.get(entry.getKey())), new Text("" + entry.getValue()));
					}
				}
				position++;
			}
		}
		
		// Question 6
		context.write(new Text("\nQuestion 6\n"), new Text(""));
		Map<String, Double> unsortedDanceabilityEnergyMap = new HashMap<String, Double>();
		for(Map.Entry<String, double[]> entry : songIDDanceabilityEnergyMap.entrySet()) {
			double sum = entry.getValue()[0] + entry.getValue()[1];
			unsortedDanceabilityEnergyMap.put(entry.getKey(), sum);
		}
		
		Map<String, Double> sortedDanceabilityEnergyMap = sortMapByFloatValue(unsortedDanceabilityEnergyMap);
		Map<String, Double> unsortedDescendingOrder = new HashMap<String, Double>();
		count = 0;
		for(Map.Entry<String, Double> entry : sortedDanceabilityEnergyMap.entrySet()) {
			if(count >= 10) break;
			
			if(songIDTitleMap.containsKey(entry.getKey())) {
				//context.write(new Text(songIDTitleMap.get(entry.getKey())), new Text("" + entry.getValue()));
				unsortedDescendingOrder.put(songIDTitleMap.get(entry.getKey()), entry.getValue());
				count++;
			}
			
//			unsortedDescendingOrder.put(entry.getKey(), entry.getValue());
//			count++;
		}
		
		Map<String, Double> sortedDescendingOrder = sortMapByFloatValueDescending(unsortedDescendingOrder);
		for(Map.Entry<String, Double> entry : sortedDescendingOrder.entrySet()) {
			if(songIDTitleMap.containsKey(entry.getKey())) {
				context.write(new Text(songIDTitleMap.get(entry.getKey())), 
						new Text(songIDDanceabilityEnergyMap.get(entry.getKey())[0] + "" + songIDDanceabilityEnergyMap.get(entry.getKey())[1]));
			}
		}
	}

	/*
	 * 
	 */
	// Sort from highest to lowest
	private HashMap<String, Integer> sortMapByValue(Map<String, Integer> unsortedMap) {
		// Create list from elements of map
		List<Map.Entry<String, Integer> > list = new LinkedList<Map.Entry<String, Integer> >(unsortedMap.entrySet());
		// Sort the list from highest to lowest. Switch o2 compareTo o1 to get lowest to highest
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});

		// Put data from sorted list to sorted hashmap by values
		HashMap<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	/*
	 * 
	 */
	// Sort from highest to lowest
	private HashMap<String, Double> sortMapByFloatValue(Map<String, Double> unsortedMap) {
		// Create list from elements of map
		List<Map.Entry<String, Double> > list = new LinkedList<Map.Entry<String, Double> >(unsortedMap.entrySet());
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
		return sortedMap;
	}
	
	/*
	 * 
	 */
	// Sort from lowest to highest
	private HashMap<String, Double> sortMapByFloatValueDescending(Map<String, Double> unsortedMap) {
		// Create list from elements of map
		List<Map.Entry<String, Double> > list = new LinkedList<Map.Entry<String, Double> >(unsortedMap.entrySet());
		// Sort the list from highest to lowest. Switch o2 compareTo o1 to get lowest to highest
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
			public int compare(Map.Entry<String, Double> o1,
					Map.Entry<String, Double> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		// Put data from sorted list to sorted hashmap by values
		HashMap<String, Double> sortedMap = new LinkedHashMap<String, Double>();
		for (Map.Entry<String, Double> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}
