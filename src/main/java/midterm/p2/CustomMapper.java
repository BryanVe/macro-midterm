package midterm.p2;

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
    String experienceLevel = rowData[1];
    String remoteRatio = rowData[8];
    String companyLocation = rowData[9];
    Text newKey = new Text(companyLocation);
    Text newValue = new Text(remoteRatio + "/" + experienceLevel + "/" + companyLocation);

    outputCollector.collect(newKey, newValue);
  }
}
