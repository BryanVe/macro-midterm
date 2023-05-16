package midterm.p8_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  final String QUERY_COMPANY_LOCATION = "CA";
  final String QUERY_JOB_TITLE = "Data Scientist";
  final String QUERY_COMPANY_SIZE = "M";
  final String QUERY_EXPERIENCE_LEVEL = "SE";

  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("work_year")) return;

    String[] rowData = value.toString().split(",");
    String experienceLevel = rowData[1].trim();
    String jobTitle = rowData[3].trim();
    String salaryInUSD = rowData[6].trim();
    String companyLocation = rowData[9].trim();
    String companySize = rowData[10].trim();

    if (
      jobTitle.equals(QUERY_JOB_TITLE) &&
      companySize.equals(QUERY_COMPANY_SIZE) &&
      companyLocation.equals(QUERY_COMPANY_LOCATION) &&
      experienceLevel.equals(QUERY_EXPERIENCE_LEVEL)
    ) {

      Text newKey = new Text(jobTitle);
      Text newValue = new Text(salaryInUSD);

      outputCollector.collect(newKey, newValue);
    }
  }
}
