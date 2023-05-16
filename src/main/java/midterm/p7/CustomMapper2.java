package midterm.p7;

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
    String[] aux = value.toString().split("/");
    String country = aux[0];
    String experienceYears = aux[1];
    String salaryAverage = aux[2];

    outputCollector.collect(new Text(country), new Text(experienceYears + "/" + salaryAverage));
  }
}
