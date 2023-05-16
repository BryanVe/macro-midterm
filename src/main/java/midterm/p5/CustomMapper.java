package midterm.p5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("work_year")) return;

    String valueString = value.toString();
    String[] rowData = valueString.split(",");
    String country = rowData[9];
    String jobTitle = rowData[3];
    String salaryInUSD = rowData[6];

    Text newKey = new Text(country + "/" + jobTitle);
    Text newValue = new Text(jobTitle + "/" + salaryInUSD + "/" + country);

    outputCollector.collect(newKey, newValue);
  }
}
