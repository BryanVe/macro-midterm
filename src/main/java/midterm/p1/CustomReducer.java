package midterm.p1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  final String QUERY_JOB_TITLE = "Data Scientist";

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    // Ahorar agrupamos por país mediante el reducer
    while (iterator.hasNext()) {
      Text value = iterator.next();
      String[] data = value.toString().split("/");
      String jobTitle = data[0];
      String salaryInUSD = data[1];

      // Y establecemos la salida como clave el país y como valor el
      // salario en USD para todos los países que tengan como título "Data Scientist"
      if (jobTitle.equals(QUERY_JOB_TITLE)) {
        Text newValue = new Text("/" + salaryInUSD);
        outputCollector.collect(key, newValue);
      }
    }
  }
}
