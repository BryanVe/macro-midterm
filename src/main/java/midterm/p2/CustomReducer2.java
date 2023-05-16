package midterm.p2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class CustomReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String[] countryAndAverage = key.toString().split("/");
    String country = countryAndAverage[0];
    String averageString = countryAndAverage[1];
    Set<String> experienceLevels = new HashSet<>();

    while (iterator.hasNext()) {
      String[] values = iterator.next().toString().split("/");
      String experienceLevel = values[0];

      experienceLevels.add(experienceLevel);
    }

    for (String experienceLevel: experienceLevels) {
      Text newValue = new Text(experienceLevel + " - " + averageString);
      outputCollector.collect(new Text(country), newValue);
    }
  }
}
