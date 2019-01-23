package ques3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * The Male Employment program takes in a file input from the HDFS directory gender_stats_data and
 * filters out the data for the percentage in change in male employment from year 2000 which is stored back into the HDFS. 
 * The output from the HDFS can be retrieved into a standard csv local file using the get command.
 * @author Tenzing Bajracharya
 */

public class MaleEmployment {
	/**
	 * This class is used to read a csv file, set the jar file name to ques3.jar and also set
	 * the mapper and reducer key/value output format types.
	 * Since the output of the mapper and reducer is different in the program. The outputValueClass is set to DoubleWritable.class. 
	 * FileInputFormat and FileOutputFormat takes in the arguments for the input and output directory for execution.
	*/
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Invalid Directory");
			System.exit(-1);
		}
		
		Job conf = new Job();
		conf.setJarByClass(MaleEmployment.class);
		
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);
		
		conf.setMapperClass(MaleEmploymentMapper.class);
		conf.setReducerClass(MaleEmploymentReducer.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		boolean success = conf.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
