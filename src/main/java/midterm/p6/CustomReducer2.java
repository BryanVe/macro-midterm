package midterm.p6;

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
  final int QUERY_SALARY_IN_USD = 200000;

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    while (iterator.hasNext()) {
      String[] data = iterator.next().toString().split("/");
      int salaryInUSD = Integer.parseInt(data[0]);
      String companySize = data[1];
      String employeeResidence = data[2];

      if (salaryInUSD > QUERY_SALARY_IN_USD) {
        Text newKey = new Text(employeeResidence);
        Text newValue = new Text(companySize + " - " + salaryInUSD);

        outputCollector.collect(newKey, newValue);
      }
    }
  }
}
