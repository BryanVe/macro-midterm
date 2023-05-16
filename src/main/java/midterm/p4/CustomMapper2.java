package midterm.p4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class CustomMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reports) throws IOException {
    // Extraemos los datos del anterior reducer
    String[] data = value.toString().split("/");
    String companyLocation = data[0].trim();
    String jobTitle = data[1].trim();
    String frequency = data[2].trim();
    Text newKey = new Text(companyLocation);
    Text newValue = new Text(jobTitle + "/" + frequency);

    // Establecemos la salida, como clave el país y como valor el título más su frecuencia
    outputCollector.collect(newKey, newValue);
  }
}
