package midterm.p4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reports) throws IOException {
    if (key.get() == 0 || value.toString().contains("work_year")) return;

    // Extraemos solo el título del trabajo y el país de la empresa
    String[] rowData = value.toString().split(",");
    String jobTitle = rowData[3].trim();
    String companyLocation = rowData[9].trim();
    Text newKey = new Text(companyLocation);
    Text newValue = new Text(jobTitle);

    // Establecemos la salida, como clave el país y como valor el país
    outputCollector.collect(newKey, newValue);
  }
}
