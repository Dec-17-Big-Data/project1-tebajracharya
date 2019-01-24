package ques5;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
* Input is a comma-separated string, interpreted as Key:Value. 
* Output is Key:Value, and the key contains the Country and value is Maternity leave (days paid) for most current year.
*/

public class maternityBenefitMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	/**
	 * The map function gets a key which is a byte offset and value is a single line from the csv file.
	 * It uses a search criteria or indicator code to filter the data and then uses a regex to split the data into words
	 * In this program i have a single loop running from backward from index of 2016 to get the data for the most recent maternity leave days for a country.
	 * The inputs are tested if it is a number or space using try catch block. If it happens to be a space the exception is caught and ignored. 
	 * Search criteria in the map phase is looking for "Maternity leave (days paid)" for most recent year.
	 * The mapped output is then cleaned and the key(country name) and value (Maternity leave (days paid)) is sent to the output.l
	 */
	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (value.toString().contains("SH.MMR.LEVE")) {			
			String line[] = value.toString().split("\",\"?");
			for(int i = line.length - 1; i > 0 ; i--) {
				try{
					context.write(new Text(line[0].toString().substring(1)), new IntWritable(Integer.parseInt(line[i])));
					i = 0;	
				}
				catch(Exception e) {
					
				}
			}
		}	
	}
}
