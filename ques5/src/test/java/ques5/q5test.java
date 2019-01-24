package ques5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class q5test {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	
	@Before
	public void setUp() {
		Mapper mapper = new maternityBenefitMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper((org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, DoubleWritable>) mapper);
	}
	 
	 @Test
	 public void testMapper() {
		 mapDriver.withInput(new LongWritable(), new Text("\"Bulgaria\",\"BGR\",\"Maternity leave (days paid)\",\"SH.MMR.LEVE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"135\",\"\",\"410\",\"\",\"410\",\"\",\"410\",\"\","));
		 mapDriver.withOutput(new Text("Bulgaria"), new DoubleWritable(410));
		 mapDriver.runTest();
	 }
}
