************Notes:

*** To run, it needs two dataset directories passed in as arguments as opposed to the one shown in the hadoop setup guide. ie:
 
$HADOOP_HOME/bin/hadoop jar build/libs/Assign3.jar cs455.hadoop.Q1.MainJob /data/metadata /data/analysis /msd/output/output-1

*** AnswersQ1-5 holds the answers to questions 1 through 5. 
	* Question 1 has every artist with their other aliases (grabbed by combining artist_id's) and their total song counts next to the set
	* Question 2 has the artist name next to their average song's loudness.
	* Question 3 has a list of songs with the hotness ratings next to them.
	* Question 4 has the artist next to their total fading-in time
	* Question 5 has a set of longest songs with seconds next to them, shortest songs with seconds next to them, and the median(s) with seconds next to it.

*** Question 6 Note:
I ran out of time trying to troubleshoot it, but essentially I stored danceability and energy values for each songID key in a map lookup table. Since the danceability and energy values were both a real number between 0 and 1, this meant that I could rate each song on a 2.0 point scale. So, the song that would as energetic and danceable as possible would be rated as a 2.0. These would then be sorted and then listed in descending order, as intended in my code.

Planned Methodology for Questions 7 through 10:
I did not make it far enough to these questions, but I did have ideas on how to approach them based on how my current code functions.

*** Question 7: 
Goal is to collect averages of every attribute for every song in the database to create one line of the average song. I will collect data from every song available in the datasets. This data would include any numbered values such as start time, pitch, timbre, max loudness, max loudness time, start loudness, song_hotness, fade in time, fade out time, duration, etc. Since this is for the average song, song titles won't be needed, so I would only use one mapper to grab data from the analysis file. 

From there, I would make multiple lookup maps in the reducer with the songID as the key and the values being the other numbered values. The reducer would then reach the cleanup phase after it is done with the reduce method, and that is where I will write only one line/segment that will contain the averages for every song, such as the average timbre of each song. 

There will need to be several checks to see if data is in the proper form when obtaining it in the mapper.

*** Question 8: 
This one will involve two mappers like in Questions 1 through 6. The mapper will go through the metadata file in order to associate artist_id with a list of their song_ids and the reducer will keep a lookup map of this. The metadata mapper will also send data for making a lookup map for artist_id and artist_hottness and a map artist_id and artist_familiarity in reducer. The analysis mapper will send data to make a lookup map of song_id's and their song_hotttnesss rating in the reducer.

With these in hand, the reducer cleanup phase will be able to write out:
	- artists with the highest and lowest hotness rating
	- artists with the highest and lowest average song hotness
	- artists with the highest and lowest familiarity rating
At least twenty entries will be written when determining the highest/lowest ratings in each section. This will allow me to analyze which artist is the most generic and which artist is the most unique with the assumption that generic implies low ratings overall and unique implies higher ratings.

*** Question 9:
For this one, I imagine one way to generate a song with a higher hotness score than in three would be to take all of the good parts of songs that have a high hotness score and generate it from there. Like in question 8, I can look at the artists with the highest artist rating, average song hotness rating, and familiarity rating and store those artists' songs separately from the rest in a map lookup table. 

With those songs, I can find the average attributes for each of the highest rating songs including the tempo, time signature, danceability, duration, mode, energy, key, loudness, time fading in/out, etc. As for the terms that describe the artist who made it, I can count every term describing every artist and select among the highest coined terms. Using information from the highest rating songs, this will be able to create a song with a higher hotness song than in three. 

***Question 10:
**Question: "As the lead data scientist for a start-up firm in Colorado that promotes and advises local musicians on becoming popular during their career. Although every individual song can be analyzed for their popularity ratings such as in question 3, 8, or 9, people listen to specific genres over others. Artists will normally have one or few genres with their music and some people will normally have genres they do not listen to, so it is essential for a music promoting/advising firm to know what makes a highly rated song in a specific genre. 

Your job is to take the FMA Dataset for Music Analysis (https://github.com/mdeff/fma), which contains data on individual songs including the specific genre they belong to. Using both the million song dataset and the FMA dataset, create a song with attributes that will be considered highly rated in a specific genre of your choosing. It is assumed that overalapping genres are acceptable.

**Approach: For this question, at least four mappers will be necessary to access each dataset: metadata and analysis in million song dataset and the tracks.csv and genres.csv in the FMA dataset. Two mappers for the million song dataset will be used to essentially contain lookup tables with information such as artist hotness rating, artist average song hotness rating, and artist familiarity rating (similar to what was done in question 9). 

The other two mappers will each split information from the tracks and genres datasets in FMA. Tracks contains information similar to the million song metadata dataset, except it also includes genres. The genres dataset contains information on all 163 possible genres. 

In the reducer, the songs can be filtered and stored in a list that contains songs from a specific genre. The title field in FMA can be compared to the title field in the million song metadata dataset, which will also allow the highest ratings to be filtered to songs in only a specific genre. From there, this will follow the same analysis as question 9, only instead of generating the overall top hottest song from every song, this will be done specifically for one genre using another dataset. 

----------------------------------------------------

cs455.hadoop.Q1.MainJob.java
* Driver class for starting setting up the two mappers and one reducer for questions 1 through 6. Only has one job and takes in both the metadata file and analysis file at once using MultipleInputs

cs455.hadoop.Q1.AnaysisFileMapper.java
* Mapper class that grabs information from the dataset found in /data/analysis

cs455.hadoop.Q1.MetadataFileMapper.java
* Mapper class that grabs information from the dataset found in /data/metadata

cs455.hadoop.Q1.Q1Reducer.java
This is where all the action is:
	* Takes in split data from both mappers to fill in several lookup maps for future reference in reduce method
	* Once reducer finishes going through the mappers, it goes through the cleanup process where it
uses the lookup maps to write answers to the questions 1-6

