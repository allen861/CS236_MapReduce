# CS236_MapReduce

0. What is included
	1. README.txt and README.pdf
	2.  run.bash
	3.  project_opti.jar
	4.  part-r-00000.txt      - reduce output with headers added
	5.  typescript.txt    	-  shows how to run
	6.  MapSideHashMapJoinMain.java          - source file
	7.  CS236_Class_Project.pdf           - description of project

1. How to run ( read typescript.txt for a demo )
	
	1.1. Unzip “Xing_Submit.zip”
	> unzip Xing_Submit.zip
	1.2. In command line, run script file “run.bash”
	>   	
	 Enter the path for folder containing location file
			e.g., "/user/sjaco002/project/input\"
	 Enter the path for folder containing recording files
	 Enter the path for output folder
	

2.  Overall description of methods
2.1. Problem analysis
	Given lines of recording data, we need to get state name and month name for each line. State name can be obtained by joining location data set with recordings data set, using "USAF" as join key. 
	For this specific project, since the number of output keys (one state per key) is 55 (53 states in US and 2 pseudo states for pacific ocean and atlantic ocean), sorting task can be integrated into reduce() task.

2.2.  Design of mapreduce job
	Our method successfully uses only one mapreduce pass to obtain ordered (in ascending order of monthly temperature variance) results including temperature, precipitation and spatial join of "CTRY" states.
	 Let map() task read recording data, output <key = state and month,  value = combined information>. Let combine() subroutine do a preliminary summation. Then, send intermediate <key,values> to reduce() task for averaging and sorting. See part 3 for details.

3.  What is done in each step of mapreduce
3.1. Setup() subroutine of map task
	Location dataset is parsed and a hashmap<key = USAF ID, value = state abbreviation > is constructed. 
3.2. Map task 
	Map() input value is one line of recording data. 
	Map() output key is text type string concatenated from state and month names, e.g. "CA01" means January in California. The total number of different keys is 55 × 12 = 660.  Month name is obtained by parsing directly. State name is obtained by lookup in hashmap<ID, state>, which has already been constructed in setup() subroutine.	
	Map() output values include temperature and precipitation information, thus a custom class named MonthInfo is used as output data type.
3.3. Reduce() task
	A hashmap< key = state, value = a custom class with combined information about this state> is built to save the results from reduce().	Therefore, reduce output key is temperature variance for a state, output value includes other information, e.g., state name, highest/lowest temperature, month names and corresponding precipitation.
	Summation and averaging is done in reduce() subroutine, but not every month’s average is saved. Hashmap is only updated when this month’s temperature is the highest/lowest value observed so far. 
3.4. Sorting in cleanup() subroutine of reduce task
	The hashmap in 3.3 is sorted in cleanup() subroutine using data structure treemap<key = temperature variance in  double type, value = a string with information about this state> and then collected as reduce output.

4.  An estimate of running time and analysis of screen output
4.1. Running time
	As can be seen in typescript.txt file, the program takes about 21 seconds clock-time to run.
4.2. Screen output
4.2.1. The number of task
	I guess the number of map tasks (25 map tasks here) is related to the default block size (64MB) for files stored in hdfs. The total size of recoding data set (2006.txt.. 2009.txt) is 1.51 GB. 
	1.51 × 1024 / 64 = 24.16,  therefore the number of map task is 25.
4.2.2. Map input records=11722666 , Map output records=3873184
	This indicates that about 33.0% of recording data is from US states or “pacific/atlantic ocean” states.
4.2.3. Combine input records=3873184, Combine output records=7073
	This indicates that combine() works and is very useful to reduce data shuffle.
4.2.4. Reduce input groups=660, Reduce input records=7073, Reduce output records=55	
	660 reduce input groups is consistent with the number of keys output by map(). 55 reduce output records is consistent with the 55 states ( 53 states in US and two ocean states).

5. A description of how you chose to do the join(s)
	Since location data set is a small file containing 82492 lines, join key/value in this small file can be saved in memory using a map data structure. When recording data is read in (join key is USAF id), state is obtained by lookup in previously constructed map.

6. Extra work
6.1. Use of Combiner to decrease shuffle bytes
	A combiner is used to decrease the amount of data shuffle. As we can see in typescript.txt, the number of output records from map() is 3873184, after preliminary summation in combine(), the number of output records from combine() is 7073. Suppose recording data is randomly distributed in every map() task, the expected number of output records should be 25 × 660 = 16500.   7073 << 16500, suggesting our recording data is partially ordered.
6.2. Faster execution time?
	In our method, monthly temperature summation and averaging is done in reduce. We can also do summation in map task using a hashmap<key = state and month name, values = sum and count>. In this way, no data shuffle is needed since there is no reduce task. However, for the purpose of learning how to use mapreduce, we write reduce task to do the summation and averaging job.
6.3. Enriching the data with precipitation
	If precipitation data is also needed, there are at least two methods:
	(1) Concatenating temperature and precipitation data into a text type string, which used as the output value of map(). Text type string is then parsed in reduce task.
	(2) Construct a new class (e.g., MonthInfo) to save temperature and precipitation data, which should override abstract methods in WritableComparable interface. This is the most difficult part of this project and takes quite a while to debug. 
6.4. Spatial join of stations with “CTRY” as “US”
	Instead of estimating a state, we think it makes more sense to create separate pseudo states “pacific oceanand” (abbreviation: PO) and “atlantic ocean(AO)”. A maximum distance corresponding three time-zones (15 per time zone) from that station to the geographical center of US ( 39.833 N, -98.583 W )1 is required. See source code for details.
	Satisfying station is assigned as either pacific ocean (PO) or atlantic ocean (AO) state based on its longitude. 
6.5. Typo found in location data set
	We found that some stations with the same ID appear multiple times in WeatherStationLocation.csv file. Especially, we found five stations with conflict state information. Data from these stations are marked as state “ZZ” (source code line 154) and ignored (source code line 204). Some of the typo can be manually fixed, for example, USAF “720313” with station name “PERRY LEFORS FIELD AIRPORT” should be in TX rather than NM.
xzhan018@well $ cat WeatherStationLocations.csv | grep "720313"
"720313","03052","PERRY LEFORS FIELD AIRPORT","US","TX","+35.613","100.996","+0989.1","20060101","20140403"
"720313","99999","PERRY LEFORS FLD","US","NM","+35.617","-100.983","+0989.0","20050101","20051231"
~
xzhan018@well $ cat WeatherStationLocations.csv | grep "722221"
"722221","13899","PENSACOLA REGIONAL AP","US","FL","+30.478","-087.187","+0036.0","",""
"722221","99999","DIXON ARPT","US","WY","+41.038","-107.497","+1987.3","20100527","20140403"
~
xzhan018@well $ cat WeatherStationLocations.csv | grep "724925"
"724925","23240","CROWS LANDING","US","CA","+37.400","-121.133","+0056.7","",""
"724925","99999","SCHRIEVER AFB","US","CO","+38.833","-104.417","+1878.0","",""
~
xzhan018@well $ cat WeatherStationLocations.csv | grep "725054"
"725054","64710","NORTH CENTRAL STATE ARPT","US","RI","+41.921","-071.491","+0134.4","20060101","20140403"
"725054","99999","NORTH CENTRAL STATE","US","CT","+41.921","-071.483","+0134.0","20050101","20051231"
~
xzhan018@well $ cat WeatherStationLocations.csv | grep "725060"
"725060","14704","NANTUCKET MEM","US","MA","+41.650","-070.517","+0040.2","20060101","20100731"
"725060","14756","NANTUCKET MEMORIAL AIRPORT","US","MA","+41.253","-070.061","+0014.6","20130101","20140403"
"725060","94728","NEW YORK CENTRAL PARK","US","NY","+40.779","-073.969","+0047.5","20100801","20120713"

	
References:	
1. http://en.wikipedia.org/wiki/Geographic_center_of_the_contiguous_United_States
