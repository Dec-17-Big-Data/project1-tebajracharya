package ques1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

import ques1.FemaleGraduatesMapper;

public class q1test {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	
	@Before
	public void setUp() {
		Mapper mapper = new FemaleGraduatesMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper((org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, DoubleWritable>) mapper);
	}
	 
	 @Test
	 public void testMapper() {
		 mapDriver.withInput(new LongWritable(), new Text("\"Nepal\",\"NPL\",\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"9.09035\",\"\",\"\",\"\","));
		 mapDriver.withOutput(new Text("Nepal"), new DoubleWritable(9.09));
		 mapDriver.runTest();
	 }
}
