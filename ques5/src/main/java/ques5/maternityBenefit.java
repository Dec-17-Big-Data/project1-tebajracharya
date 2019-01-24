package ques5;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The Female Employment program takes in a file input from the HDFS directory gender_stats_data and
 * filters out the data for the maternity days leave for most current year which is stored back into the HDFS. 
 * The output from the HDFS can be retrieved into a standard csv local file using the get command.
 * @author Tenzing Bajracharya
 */

public class maternityBenefit {
	/**
	 * This class is used to read a csv file, set the jar file name to que5.jar and also set
	 * the reducer to 0 since this search criteria did not need additional reducing.
	 * Since the output of the mapped and reducer is different in the program. The outputValueClass is set to IntWritable.class. 
	 * FileInputFormat and FileOutputFormat takes in the arguments for the input and output directory for execution.
	*/
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Invalid Directory");
			System.exit(-1);
		}
		
		Job conf = new Job();
		conf.setJarByClass(maternityBenefit.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(maternityBenefitMapper.class);
		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		boolean success = conf.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}

}