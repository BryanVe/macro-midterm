package midterm.p5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    // create a map for saving job with its mean salary
    int sum = 0;
    int counter = 0;
    String[] countryAndJobTitle = key.toString().split("/");
    String country = countryAndJobTitle[0];
    String jobTitle = countryAndJobTitle[1];
    List<String> jobTitles = new ArrayList<>();

    while (iterator.hasNext()) {
      String[] values = iterator.next().toString().split("/");
      String job = values[0];
      int salary = Integer.parseInt(values[1]);
      String currentCountry = values[2];

      if (country.equals(currentCountry)) {
        jobTitles.add(job);
      }

      if (jobTitle.equals(job)) {
        sum += salary;
        counter += 1;
      }
    }

    float average = (float) sum / counter;

    for (String jt: jobTitles) {
      Text newValue = new Text("/" + jt);
      outputCollector.collect(new Text(country + "/" + average), newValue);
    }
  }
}
