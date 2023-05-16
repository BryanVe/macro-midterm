package midterm.p6;

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
  final String QUERY_COMPANY_LOCATION = "US";
  final String QUERY_JOB_TITLE = "Data Analyst";
  final String QUERY_EMPLOYMENT_TYPE = "FT";

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String companyLocation = key.toString();

    if (companyLocation.equals(QUERY_COMPANY_LOCATION)) {
      while (iterator.hasNext()) {
        String[] data = iterator.next().toString().split("/");
        String jobTitle = data[0];
        String salaryInUSD = data[1];
        String employmentType = data[2];
        String companySize = data[3];
        String employeeResidence = data[4];

        if (jobTitle.equals(QUERY_JOB_TITLE) && employmentType.equals(QUERY_EMPLOYMENT_TYPE)) {
          Text newKey = new Text(jobTitle);
          Text newValue = new Text("/" + salaryInUSD + "/" + companySize + "/" + employeeResidence);

          outputCollector.collect(newKey, newValue);
        }
      }
    }
  }
}
