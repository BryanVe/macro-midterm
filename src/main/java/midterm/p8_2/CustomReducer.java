package midterm.p8_2;

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
    int minSalaryInUSD = Integer.MAX_VALUE;

    while (iterator.hasNext()) {
      int salaryInUSD = Integer.parseInt(iterator.next().toString());

      if (salaryInUSD < minSalaryInUSD) {
        minSalaryInUSD = salaryInUSD;
      }
    }

    Text newKey = new Text(Integer.toString(minSalaryInUSD));
    Text newValue = new Text("");
    outputCollector.collect(newKey, newValue);
  }
}
