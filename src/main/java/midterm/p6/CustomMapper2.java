package midterm.p6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class CustomMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String[] data = value.toString().split("/");
    String jobTitle = data[0].trim();
    String salaryInUSD = data[1].trim();
    String companySize = data[2].trim();
    String employeeResidence = data[3].trim();

    Text newKey = new Text(jobTitle);
    Text newValue = new Text(salaryInUSD + "/" + companySize + "/" + employeeResidence);

    outputCollector.collect(newKey, newValue);
  }
}
