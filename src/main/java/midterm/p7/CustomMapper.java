package midterm.p7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.time.Year;

public class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("work_year")) return;

    String valueString = value.toString();
    String[] rowData = valueString.split(",");
    int experienceYears = Year.now().getValue() - Integer.parseInt(rowData[0]);
    String salary = rowData[6];
    String jobTitle = rowData[3];
    String country = rowData[9];

    Text newKey = new Text(jobTitle + "/" + experienceYears + "/" + country);
    Text newValue = new Text(salary + "/" + jobTitle);

    outputCollector.collect(newKey, newValue);
  }
}
