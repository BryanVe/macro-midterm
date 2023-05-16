package midterm.p8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class CustomReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  final String QUERY_JOB_TITLE = "Data Scientist";

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String jobTitle = key.toString();

    while (iterator.hasNext()) {
      String[] data = iterator.next().toString().split("/");
      String companySize = data[2];

      if (jobTitle.equals(QUERY_JOB_TITLE)) {
        Text newKey = new Text(companySize);
        Text newValue = new Text("/" + data[0] + "/" + data[1]);

        outputCollector.collect(newKey, newValue);
      }
    }
  }
}
