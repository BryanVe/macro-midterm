package midterm.p1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

class CustomMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    // Ahora obtenemos el país y el salario en USD
    String valueAsString = value.toString();
    String[] data = valueAsString.split("/");
    String companyLocation = data[0].trim();
    String salaryInUSD = data[1].trim();
    Text newKey = new Text(companyLocation);
    Text newValue = new Text(salaryInUSD);

    outputCollector.collect(newKey, newValue);
  }
}
