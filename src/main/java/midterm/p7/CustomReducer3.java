package midterm.p7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class CustomReducer3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  private boolean done = false;

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    int sum = 0;
    int count = 0;

    while (iterator.hasNext()) {
      sum += Float.parseFloat(iterator.next().toString().trim());
      count++;
    }

    float avg = (float) sum / count;

    if (!done) {
      outputCollector.collect(new Text(String.valueOf(avg)), new Text(""));
      done = true;
    }
  }
}
