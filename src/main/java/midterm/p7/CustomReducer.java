package midterm.p7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  private final String JOB_TITLE = "Principal Data Scientist";

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    // create a map for saving the Principal Data Scientist salary with 3 years of experience
    int sum = 0;
    int counter = 0;
    String[] jobTitleWithExperienceYearsAndCountry = key.toString().split("/");
    String jobTitle = jobTitleWithExperienceYearsAndCountry[0];
    String experienceYears = jobTitleWithExperienceYearsAndCountry[1];
    String country = jobTitleWithExperienceYearsAndCountry[2];

    while (iterator.hasNext()) {
      String[] values = iterator.next().toString().split("/");
      String salaryString = values[0];
      String jt = values[1];
      int salary = Integer.parseInt(salaryString);

      if (JOB_TITLE.equals(jt)) {
        sum += salary;
        counter += 1;
      }
    }

    float average = (float) sum / counter;

    if (JOB_TITLE.equals(jobTitle)) {
      Text newValue = new Text("/" + average);
      outputCollector.collect(new Text(country + "/" + experienceYears), newValue);
    }
  }
}
