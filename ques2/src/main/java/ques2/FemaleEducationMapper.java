package ques2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
* Input is a comma-separated string, interpreted as Key:Value. 
* Output is Key:Value pair, and the key search criteria's and the value is avg increase in female education from 2000 using school enrollment
* and  gross graduation ratio.
*/

public class FemaleEducationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	/**
	 * The map function gets a key which is a byte offset and value is a single line from the csv file.
	 * It uses a search criteria or indicator code to filter the data and then uses a regex to split the data into words
	 * which is then iterated from the year 2000 using index value till the most recent year to average increase in female education each year. 
	 * The inputs are tested if it is a number or space. If it is a space the exception is caught and ignored.
	 * Search criteria in the mapper phase is looking for "Gross graduation ratio, tertiary, female (%)" and  
	 * "School enrollment, tertiary, female (% gross)" to determine the increase in average in female education.
	 * The matching search line is then cleaned in the mapper and the average increase in determined for each year (rounded to 3 decimal places)
	 * and output to the output file which is stored back in the HDFS and can be retried to the local drive.
	 */
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if ((value.toString().contains("SE.TER.CMPL.FE.ZS") && value.toString().contains("United States")) || value.toString().contains("SE.TER.ENRR.FE") && value.toString().contains("United States"))
			{		
			String line[] = value.toString().split("\",\"?");
			Double percentageChange = 0.0;
			Double currentYear = 0.0;
			Double nextYear = 0.0;
			for(int i = 44;i < line.length - 1; i++) {
				try{
					currentYear = Double.parseDouble(line[i]);
					nextYear = Double.parseDouble(line[i+1]);
					percentageChange = ((nextYear - currentYear) / currentYear);
					context.write(new Text(line[2].toString()), new DoubleWritable(Math.round(percentageChange *1000.0)/1000.0));
				}
				catch(Exception e) {
				}
			}
		}
	}	
}
