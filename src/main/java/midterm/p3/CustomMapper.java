package midterm.p3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("work_year")) return;

    // Extraemos solo el título del trabajo, su salario en USD y el tamaño de la empresa
    String valueAsString = value.toString();
    String[] rowData = valueAsString.split(",");
    String jobTitle = rowData[3].trim();
    String salaryInUSD = rowData[6].trim();
    String companySize = rowData[10].trim();
    Text newKey = new Text(companySize);
    Text newValue = new Text(jobTitle + "/" + salaryInUSD);

    // Establecemos la salida, como clave el tamaño y como valor el título más su salario en USD
    outputCollector.collect(newKey, newValue);
  }
}
