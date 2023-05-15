package midterm.p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("work_year")) return;

    String valueString = value.toString();
    String[] rowData = valueString.split(",");
    String jobTitle = rowData[3].trim();
    String salary = rowData[6].trim();
    Text newKey = new Text(jobTitle);
    Text newValue = new Text(salary);

    outputCollector.collect(newKey, newValue);
  }
}
