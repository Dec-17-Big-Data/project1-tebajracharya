package ques4;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import ques4.FemaleEmploymentMapper;
import ques4.FemaleEmploymentReducer;

public class q4test {
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {
		
		FemaleEmploymentMapper mapper = new FemaleEmploymentMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper((Mapper<LongWritable, Text, Text, Text>) mapper);

		FemaleEmploymentReducer reducer = new FemaleEmploymentReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
		reduceDriver.setReducer((Reducer<Text, Text, Text, DoubleWritable>) reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable>();
		mapReduceDriver.setMapper((Mapper<LongWritable, Text, Text, Text>) mapper);
		mapReduceDriver.setReducer((Reducer<Text, Text, Text, DoubleWritable>) reducer);
	}

	@Test
	public void testMapper() {

		mapDriver.withInput(new LongWritable(1), new Text("\"Algeria\",\"DZA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.82100009918213\",\"5.43300008773804\",\"5.55499982833862\",\"5.44899988174438\",\"4.78999996185303\",\"4.59999990463257\",\"5.58300018310547\",\"5.55900001525879\",\"5.54899978637695\",\"4.69600009918213\",\"5.38500022888184\",\"5.87099981307983\",\"6.61800003051758\",\"8.48999977111816\",\"9.27999973297119\",\"10.2589998245239\",\"10.1949996948242\",\"11.0459995269775\",\"11.6099996566772\",\"11.5900001525879\",\"12.6029996871948\",\"13.3830003738403\",\"14.1029996871948\",\"13.8360004425049\",\"13.6719999313354\",\"13.5909996032715\","));

		mapDriver.withOutput(new Text("Algeria"), new Text("13.591,4.696"));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {

		List<Text> values = new ArrayList<Text>();
		values.add(new Text("13.591,4.696"));

		reduceDriver.withInput(new Text("Algeria"), values);

		reduceDriver.withOutput(new Text("Algeria"), new DoubleWritable(1.349));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {

		mapReduceDriver.withInput(new LongWritable(1), new Text("\"Algeria\",\"DZA\",\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.82100009918213\",\"5.43300008773804\",\"5.55499982833862\",\"5.44899988174438\",\"4.78999996185303\",\"4.59999990463257\",\"5.58300018310547\",\"5.55900001525879\",\"5.54899978637695\",\"4.69600009918213\",\"5.38500022888184\",\"5.87099981307983\",\"6.61800003051758\",\"8.48999977111816\",\"9.27999973297119\",\"10.2589998245239\",\"10.1949996948242\",\"11.0459995269775\",\"11.6099996566772\",\"11.5900001525879\",\"12.6029996871948\",\"13.3830003738403\",\"14.1029996871948\",\"13.8360004425049\",\"13.6719999313354\",\"13.5909996032715\","));

		mapReduceDriver.addOutput(new Text("Algeria"), new DoubleWritable(1.349));
		
		mapReduceDriver.runTest();
	}
}
