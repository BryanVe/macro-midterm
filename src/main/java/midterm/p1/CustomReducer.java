package midterm.p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    // create map for saving payment types with its total prices
    int sum = 0;
    int counter = 0;

    while (iterator.hasNext()) {
      Text value = iterator.next();
      String salary = value.toString();
      sum += Integer.parseInt(salary);
      counter += 1;
    }

    Text newValue = new Text("/" + (float) sum / counter);

    outputCollector.collect(key, newValue);
  }
}
