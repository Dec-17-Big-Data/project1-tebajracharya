package ques1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
* The female graduates program takes in a input file from the gender-_stats_data folder from the HDFS and 
* filters the percentage of female graduates by country and stores the output back into the HDFS. 
* The output from the HDFS can be retrieved into a standard csv local file.
*
* @author  Tenzing Bajracharya 
*/

public class FemaleGraduates {
	/**
	 * This class is used to read a csv file, set the jar file name to ques1.jar and also set
	 * the mapper and reducer key/value output format types.
	 * Since there is no reducer needed in this program the number of reducers is set to 0. FileInputFormat and
	 * FileOutputFormat takes in the arguments for the input and output directory for execution.
	*/
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Invalid Directory");
			System.exit(-1);
		}
		
		Job conf = new Job();
		conf.setJarByClass(FemaleGraduates.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(FemaleGraduatesMapper.class);
		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		boolean success = conf.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
