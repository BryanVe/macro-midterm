package midterm.p8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  final String QUERY_COMPANY_LOCATION = "CA";

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String companyLocation = key.toString();

    while (iterator.hasNext()) {
      String[] data = iterator.next().toString().split("/");
      String jobTitle = data[0];

      if (companyLocation.equals(QUERY_COMPANY_LOCATION)) {
        Text newKey = new Text(jobTitle);
        Text newValue = new Text("/" + data[1] + "/" + data[2] + "/" + data[3]);

        outputCollector.collect(newKey, newValue);
      }
    }
  }
}
