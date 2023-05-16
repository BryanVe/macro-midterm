package midterm.p2;

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
    // create a map for saving experience_level with its remote_ratio
    int sum = 0;
    int counter = 0;
    String[] countryAndExperienceLevel = key.toString().split("/");
    String country = countryAndExperienceLevel[0];
    String experienceLevel = countryAndExperienceLevel[1];
    List<String> experienceLevels = new ArrayList<>();

    while (iterator.hasNext()) {
      String[] values = iterator.next().toString().split("/");
      String remoteRatioString = values[0];
      String el = values[1];
      String companyLocation = values[2];
      int remoteRatio = Integer.parseInt(remoteRatioString);

      if (country.equals(companyLocation)) {
        experienceLevels.add(el);
      }

      if (experienceLevel.equals(el)) {
        sum += remoteRatio;
        counter += 1;
      }
    }

    float average = (float) sum / counter;

    for (String el: experienceLevels) {
      Text newValue = new Text("/" + el);
      outputCollector.collect(new Text(country + "/" + average), newValue);
    }
  }
}
