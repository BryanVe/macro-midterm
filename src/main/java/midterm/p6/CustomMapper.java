package midterm.p6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("work_year")) return;

    String[] rowData  = value.toString().split(",");
    String employmentType = rowData[2].trim().trim();
    String jobTitle = rowData[3].trim();
    String salaryInUSD = rowData[6].trim();
    String employeeResidence = rowData[7].trim();
    String companyLocation = rowData[9].trim();
    String companySize = rowData[10].trim();

    Text newKey = new Text(companyLocation);
    Text newValue = new Text(
jobTitle + "/" +
      salaryInUSD + "/" +
      employmentType + "/" +
      companySize + "/" +
      employeeResidence
    );

    outputCollector.collect(newKey, newValue);
  }
}
