package midterm.p8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class CustomReducer3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  final String QUERY_COMPANY_SIZE = "M";
  final String QUERY_EXPERIENCE_LEVEL = "SE";

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String companySize = key.toString();
    int minSalaryInUSD = Integer.MAX_VALUE;

    while (iterator.hasNext()) {
      String[] data = iterator.next().toString().split("/");
      int salaryInUSD = Integer.parseInt(data[0]);
      String experienceLevel = data[1];

      if (companySize.equals(QUERY_COMPANY_SIZE) && experienceLevel.equals(QUERY_EXPERIENCE_LEVEL)) {
        if (salaryInUSD < minSalaryInUSD) {
          minSalaryInUSD = salaryInUSD;
        }

      }
    }

    if (companySize.equals(QUERY_COMPANY_SIZE)){
      Text newKey = new Text(Integer.toString(minSalaryInUSD));
      Text newValue = new Text("");

      outputCollector.collect(newKey, newValue);
    }
  }
}
