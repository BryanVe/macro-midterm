package midterm.p7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class CustomReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  private final String COUNTRY = "DE";
  private final int EXPERIENCE = 3;

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String country = key.toString();

    while (iterator.hasNext()) {
      String[] currentExperienceAndSalary = iterator.next().toString().split("/");
      int currentExperience = Integer.parseInt(currentExperienceAndSalary[0].trim());
      String currentSalary = currentExperienceAndSalary[1].trim();

      if (country.equals(COUNTRY) && currentExperience == EXPERIENCE) {
        Text newValue = new Text("/" + currentSalary);
        outputCollector.collect(new Text(currentSalary), newValue);
      }
    }
  }
}
