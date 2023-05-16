package midterm.p7_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    int sum = 0;
    int frequency = 0;

    while (iterator.hasNext()) {
      int salary = Integer.parseInt(iterator.next().toString());

      sum += salary;
      frequency += 1;
    }

    float average = (float) sum / frequency;
    Text newValue = new Text(Float.toString(average));
    outputCollector.collect(key, newValue);
  }
}
