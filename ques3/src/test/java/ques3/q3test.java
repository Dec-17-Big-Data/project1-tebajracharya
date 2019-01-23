package ques3;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class q3test {
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {
		
		MaleEmploymentMapper mapper = new MaleEmploymentMapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper((Mapper<LongWritable, Text, Text, Text>) mapper);

		MaleEmploymentReducer reducer = new MaleEmploymentReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
		reduceDriver.setReducer((Reducer<Text, Text, Text, DoubleWritable>) reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable>();
		mapReduceDriver.setMapper((Mapper<LongWritable, Text, Text, Text>) mapper);
		mapReduceDriver.setReducer((Reducer<Text, Text, Text, DoubleWritable>) reducer);
	}

	@Test
	public void testMapper() {

		mapDriver.withInput(new LongWritable(1), new Text("\"Argentina\",\"ARG\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"72.4820022583008\",\"71.3369979858398\",\"70.0550003051758\",\"68.1269989013672\",\"63.3699989318848\",\"64.0169982910156\",\"65.5490036010742\",\"66.370002746582\",\"64.8399963378906\",\"63.7859992980957\",\"61.984001159668\",\"60.3759994506836\",\"62.7879981994629\",\"67.2590026855469\",\"68.8499984741211\",\"70.5920028686523\",\"70.3399963378906\",\"69.8040008544922\",\"68.786003112793\",\"69.879997253418\",\"69.5749969482422\",\"70.1029968261719\",\"69.859001159668\",\"69.6750030517578\",\"70.1159973144531\",\"70.1350021362305\","));

		mapDriver.withOutput(new Text("Argentina"), new Text("44.777,35.586"));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() {

		List<Text> values = new ArrayList<Text>();
		values.add(new Text("44.777,35.586"));

		reduceDriver.withInput(new Text("Argentina"), values);

		reduceDriver.withOutput(new Text("Argentina"), new DoubleWritable(-0.033));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {

		mapReduceDriver.withInput(new LongWritable(1), new Text("\"Argentina\",\"ARG\",\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"72.4820022583008\",\"71.3369979858398\",\"70.0550003051758\",\"68.1269989013672\",\"63.3699989318848\",\"64.0169982910156\",\"65.5490036010742\",\"66.370002746582\",\"64.8399963378906\",\"63.7859992980957\",\"61.984001159668\",\"60.3759994506836\",\"62.7879981994629\",\"67.2590026855469\",\"68.8499984741211\",\"70.5920028686523\",\"70.3399963378906\",\"69.8040008544922\",\"68.786003112793\",\"69.879997253418\",\"69.5749969482422\",\"70.1029968261719\",\"69.859001159668\",\"69.6750030517578\",\"70.1159973144531\",\"70.1350021362305\","));

		mapReduceDriver.addOutput(new Text("Argentina"), new DoubleWritable(-0.033));
		
		mapReduceDriver.runTest();
	}
}
