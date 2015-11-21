package com.mj.mapSideHashMapJoin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapSideHashMapJoinMain extends Configured implements Tool {
	private static final Logger logger = LoggerFactory
			.getLogger(MapSideHashMapJoinMain.class);

	// creat a class as outputValue of map task,
	// must override write(), readFields and compareTo() methods to become
	// WritableComparable
	static class MonthInfo implements WritableComparable<MonthInfo> {
		DoubleWritable sum;
		DoubleWritable totalPrcp;
		IntWritable count;

		public MonthInfo() {
			sum = new DoubleWritable();
			totalPrcp = new DoubleWritable();
			count = new IntWritable();
		}

		public void write(DataOutput out) throws IOException {
			sum.write(out);
			totalPrcp.write(out);
			count.write(out);
		}

		public void readFields(DataInput in) throws IOException {
			sum.readFields(in);
			totalPrcp.readFields(in);
			count.readFields(in);
		}

		public int compareTo(MonthInfo other) {
			int compareVal = this.sum.compareTo(other.sum);
			if (compareVal != 0)
				return compareVal;
			return this.count.compareTo(other.count);
		}
	}

	// inputValue is one line of file
	// outputKey is state+month (e.g., CA01 means california January)
	// outputValue is MonthInfo class
	public static class JoinMapper extends
			Mapper<Object, Text, Text, MonthInfo> {

		// a hashmap with key as station id, value as state name
		private HashMap<String, String> id_state = new HashMap<String, String>();
		private Text outPutKey = new Text();
		private String mapInputStr = null;
		private String mapInputSpit[] = null;
		int len, i, j;
		String state, id, str_month, str_prcp;
		double temperature, prcp;
		private MonthInfo mi = new MonthInfo();
		// parse location of latitude and longitude
		// e.g. "+68.000","+015.617"  , there might be leading zeros
		public double parseCoordinate ( String str ){
			boolean isNegative = false;
			double res;
			int i = 1, start_index;
			if( str.charAt( 0 ) == '-' )
				isNegative = true;	
			// find the start index of number
			while( true ){
				if( str.charAt(i) != '0') {
					if( str.charAt(i) == '.' )
						start_index = i - 1;
					else
						start_index = i; 
					break;
				}
				++i;
			}
			res = Double.parseDouble( str.substring( start_index ) ) ;
			if( isNegative )
				return -res;
			return res;
		}
		// build hashmap< id, state> in setup step of map task
		protected void setup(Context context) throws IOException,
				InterruptedException {
			BufferedReader br = null;
			Path[] distributePaths = DistributedCache
					.getLocalCacheFiles(context.getConfiguration());
			String line, country, str_latitude, str_longitude;
			double distance, latitude = 0, longitude = 0;
			// the geographical center of US is 39.833 N  -98.583 W
			double centerLatitude = 39.833, centerLongitude = -98.583;
			// index array records the locations of " symbol in each line
			// we only need to parse at most the first 7 fields, so set the size as 14
			int[] index = new int[14];
			for (Path p : distributePaths) {
				if (p.toString().endsWith("WeatherStationLocations.csv")) {
					br = new BufferedReader(new FileReader(p.toString()));
					// "USAF","WBAN","STATION NAME","CTRY","STATE","LAT","LON","ELEV(M)","BEGIN","END"
					// "007005","99999","CWOS 07005","","","","","","20120127","20120127"
					// read header line
					line = br.readLine();
					// line parser
					while (null != (line = br.readLine())) {
						len = line.length();
						j = 0;
						id = null;
						boolean isUS = false;
						for (i = 0; i < len; ++i) {
							if (line.charAt(i) == '"') {
								index[j++] = i;
							} else
								continue;
							if (j == 2) {
								id = line.substring(index[0] + 1, index[1]);
								if (id.equals("999999")) // a station without ID
									break;
							} else if (j == 8) {
								country = line
										.substring(index[6] + 1, index[7]);
								if ( country.isEmpty() )
									break;
								else if ( country.equals("US") )
									isUS = true;							
							} else if (j == 10 && isUS ) {
								state = line.substring(index[8] + 1, index[9]);
								if (!state.isEmpty()) {
									if (id_state.get(id) == null)
										id_state.put(id, state);
									// due to errors in raw file, one id can
									// correspond to different states
									// for these stations, label its value as
									// "ZZ"
									else if (!id_state.get(id).equals(state))
										id_state.put(id, "ZZ");
								}
								break;
							} else if ( j == 12 ) {
								str_latitude =  line.substring(index[10] + 1, index[11]) ;
								if( str_latitude.length() < 4 )
									break;
								latitude = parseCoordinate ( str_latitude );							
							}
							else if ( j == 14 ){
								str_longitude =  line.substring(index[12] + 1, index[13]) ;
								if( str_longitude .length() < 4 )
									break;
								longitude  = parseCoordinate ( str_longitude );
								// the geographical center of US is 39.833 N  -98.583 W
								distance = Math.sqrt( Math.pow(latitude - centerLatitude , 2) + Math.pow(longitude - centerLongitude, 2) );
								// Only consider distance that corresponds to the range of three time zones, one time zone = 15 longitude
								if( distance < 45 ) {
									if( longitude < centerLongitude )
										state = "PO"; // pseudo state of Pacific Ocean
									else
										state = "AO"; // pseudo state of Atlantic Ocean
									id_state.put(id, state);									
								}								
								break;
							}
						}
					}
				}
			}
		}

		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value == null || value.toString().equals(""))
				return;
			mapInputStr = value.toString();
			len = mapInputStr.length();
			// return if value is title line
			if (len >= 6 && mapInputStr.substring(0, 6).equals("STN---"))
				return;
			mapInputSpit = mapInputStr.split(" +");
			if (mapInputSpit.length < 21)
				return;
			id = mapInputSpit[0];
			if ((state = id_state.get(id)) == null)
				return;
			// error date
			if (state.equals("ZZ"))
				return;
			str_month = mapInputSpit[2].substring(4, 6);
			str_prcp = mapInputSpit[19];
			if (str_prcp.equals("99.99"))
				prcp = 0;
			else {
				len = str_prcp.length();
				str_prcp = str_prcp.substring(0, len - 1);
				prcp = Double.parseDouble(str_prcp);
			}
			outPutKey.set(state + str_month);
			temperature = Double.parseDouble(mapInputSpit[3]);
			mi.count.set(1);
			mi.sum.set(temperature);
			mi.totalPrcp.set(prcp);
			context.write(outPutKey, mi);
		}
	}

	// Combiner for each map task
	public static class JoinCombiner extends
			Reducer<Text, MonthInfo, Text, MonthInfo> {
		private Text output = new Text();
		private MonthInfo mi = new MonthInfo();

		protected void reduce(Text key, Iterable<MonthInfo> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0, total = 0; // sum for temperature, total for
										// precipitation
			int count = 0;
			for (MonthInfo value : values) {
				sum += value.sum.get();
				total += value.totalPrcp.get();
				count += value.count.get();
			}
			mi.count.set(count);
			mi.sum.set(sum);
			mi.totalPrcp.set(total);
			context.write(key, mi);
		}
	}

	// reduce task
	public static class JoinReducer extends
			Reducer<Text, MonthInfo, Text, Text> {
		private Text output = new Text();

		// MinMaxOutput is used to record the max and min temperature for each
		// state
		static class MinMaxOutput {
			String minMonth;
			String maxMonth;
			double minAveTmp;
			double maxAveTmp;
			double minAvePrcp;
			double maxAvePrcp;

			public MinMaxOutput(String month, double temperature, double prcp) {
				minMonth = month;
				maxMonth = month;
				minAveTmp = temperature;
				maxAveTmp = temperature;
				minAvePrcp = prcp;
				maxAvePrcp = prcp;
			}
		}

		// each state's information is saved in HashMap< state, MinMaxOutput >
		HashMap<String, MinMaxOutput> map_state_info = new HashMap<String, MinMaxOutput>();
		String[] monthNames = { "January", "February", "March", "April", "May",
				"June", "July", "August", "September", "October", "November",
				"December" };

		protected void reduce(Text key, Iterable<MonthInfo> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0, total = 0; // sum for temperature, total for
										// precipitation
			int count = 0;

			for (MonthInfo value : values) {
				sum += value.sum.get();
				total += value.totalPrcp.get();
				count += value.count.get();
			}
			double aveTemp = sum / count;
			double avePrcp = total / count;
			String state = key.toString().substring(0, 2);
			String str_month = key.toString().substring(2, 4);
			if (str_month.charAt(0) == '0')
				str_month = str_month.substring(1, 2);
			int int_month = Integer.parseInt(str_month);
			str_month = monthNames[int_month - 1];
			if (map_state_info.get(state) == null) {
				map_state_info.put(state, new MinMaxOutput(str_month, aveTemp,
						avePrcp));
			} else {
				MinMaxOutput mm = map_state_info.get(state);
				if (aveTemp < mm.minAveTmp) {
					mm.minAveTmp = aveTemp;
					mm.minAvePrcp = avePrcp;
					mm.minMonth = str_month;
				} else if (aveTemp > mm.maxAveTmp) {
					mm.maxAveTmp = aveTemp;
					mm.maxAvePrcp = avePrcp;
					mm.maxMonth = str_month;
				}
			}
		}

		// cleanup will calculate the temperature differnece, and use the
		// differenc as
		// key value in Treemap to sort
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Set<String> states = map_state_info.keySet();
			Text outputValue = new Text();
			Text outputKey = new Text();
			TreeMap<Double, String> map_diff_info = new TreeMap<Double, String>();

			double diff;
			String treeMapValue;
			for (String state : states) {
				MinMaxOutput mm = map_state_info.get(state);
				diff = Double.parseDouble(String.format("%.3f", mm.maxAveTmp
						- mm.minAveTmp));
				treeMapValue = state + "," + mm.maxMonth + ","
						+ String.format("%.3f", mm.maxAveTmp) + ","
						+ String.format("%.3f", mm.maxAvePrcp) + ","
						+ mm.minMonth + ","
						+ String.format("%.3f", mm.minAveTmp) + ","
						+ String.format("%.3f", mm.minAvePrcp);
				map_diff_info.put(diff, treeMapValue);
			}
			Iterator<Map.Entry<Double, String>> it = map_diff_info.entrySet()
					.iterator();
			while (it.hasNext()) {
				Map.Entry<Double, String> entry = (Map.Entry<Double, String>) it
						.next();
				outputKey.set(entry.getKey().toString());
				outputValue.set(entry.getValue());
				context.write(outputKey, outputValue);
			}
		}
	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		DistributedCache.addCacheFile(new Path(args[1]).toUri(), conf);

		Job job = new Job(conf, "CS236");
		job.setJarByClass(MapSideHashMapJoinMain.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(JoinMapper.class);
		job.setCombinerClass(JoinCombiner.class);
		job.setReducerClass(JoinReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MonthInfo.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		try {
			int returnCode = ToolRunner.run(new MapSideHashMapJoinMain(), args);
			System.exit(returnCode);
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}
}
