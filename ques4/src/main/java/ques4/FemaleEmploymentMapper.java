package ques4;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
* Input is a comma-separated string, interpreted as Key:Value. 
* Output is Key:Value, and the key contains the Country and value is a pair of both most recent and year 2000 employment
* to population ratio, 15+, female (%) (modeled ILO estimate)
*/

public class FemaleEmploymentMapper extends Mapper<LongWritable, Text, Text, Text> {
	/**
	 * The map function gets a key which is a byte offset and value is a single line from the csv file.
	 * It uses a search criteria or indicator code to filter the data and then uses a regex to split the data into words
	 * In this program i have 2 loops running from index 44 or year 2000 to 2016 and from 2000 to 2000 to get both the most current and closest
	 * values to year 2000 to calculate the female employment percent. The inputs are tested if it is a number or space. 
	 * If it is a space the exception is caught and ignored. Search criteria in the map phase is looking for 
	 * "Employment to population ratio, 15+, female (%) (modeled ILO estimate)" to determine the female employment percentage for 2000 and 2016.
	 * The mapped output is then cleaned and the key(country name) and value (percentage for 2000 and 2016) is sent to the reducer.
	 */
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (value.toString().contains("SL.EMP.TOTL.SP.FE.ZS")) {			
			String line[] = value.toString().split("\",\"?");
			Double toMostCurrentPercentage = 0.0;
			Double fromNeededPercentage = 0.0;
				for(int i = line.length - 1; i > 0; i--) {
					try{
						toMostCurrentPercentage = Double.parseDouble(line[i]);	
					}
					catch(Exception e) {
					}
				}
				for(int i = 44;i < line.length - 1; i++) {
					try{
						fromNeededPercentage = Double.parseDouble(line[i]);	
					}
					catch(Exception e) {
					}
				}
				String val = String.valueOf(toMostCurrentPercentage) + "," + String.valueOf(fromNeededPercentage);
				Text output = new Text();
				output.set(val);
				context.write(new Text(line[0].toString().substring(1)), new Text(val));		
		}
	}		
}
