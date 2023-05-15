package midterm.p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

class CustomReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  String maxJobTitle = "";
  float maxValue = Float.MIN_VALUE;

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    Text value = iterator.next();
    float salary = Float.parseFloat(value.toString());

    if (salary > maxValue) {
      maxValue = salary;
      maxJobTitle = key.toString();
    }

    Text newKey = new Text(maxJobTitle);
    Text newValue = new Text(Float.toString(maxValue));

    outputCollector.collect(newKey, newValue);
  }
}
