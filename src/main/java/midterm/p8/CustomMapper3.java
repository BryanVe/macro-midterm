package midterm.p8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class CustomMapper3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String[] data = value.toString().split("/");
    String companySize = data[0].trim();
    String salaryInUSD = data[1].trim();
    String experienceLevel = data[2].trim();

    outputCollector.collect(new Text(companySize), new Text(salaryInUSD + "/" + experienceLevel));
  }
}
