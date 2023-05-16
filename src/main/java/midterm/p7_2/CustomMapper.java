package midterm.p7_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.time.Year;

public class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  final int QUERY_EXPERIENCE_YEARS = 3;
  final String QUERY_JOB_TITLE = "Principal Data Scientist";
  final String QUERY_COUNTRY = "DE";

  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("work_year")) return;

    String[] rowData = value.toString().split(",");
    int experienceYears = Year.now().getValue() - Integer.parseInt(rowData[0]);
    String salary = rowData[6];
    String jobTitle = rowData[3];
    String country = rowData[9];

    if (
      jobTitle.equals(QUERY_JOB_TITLE) &&
      country.equals(QUERY_COUNTRY) &&
      experienceYears == QUERY_EXPERIENCE_YEARS
    ) {
      Text newKey = new Text(jobTitle);
      Text newValue = new Text(salary);

      outputCollector.collect(newKey, newValue);
    }
  }
}
