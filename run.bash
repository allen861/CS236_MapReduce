#!/bin/bash
echo -n "Enter folder containing the Locations file, e.g.,\"/user/xzhan018/project/input\", > "
read folder_location
echo "You entered: $folder_location"
echo -n "Enter folder containing the Recordings files> "
read folder_recordings
echo "You entered: $folder_recordings"
echo -n "Enter output folder> "
read folder_output
echo "You entered: $folder_output"
hdfs dfs -rm -r $folder_output
hadoop jar project_opti.jar com.mj.mapSideHashMapJoin.MapSideHashMapJoinMain $folder_recordings $folder_location/WeatherStationLocations.csv $folder_output
hdfs dfs -ls $folder_output
hdfs dfs -cat $folder_output/part-r-00000
