package midterm.config;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Config {
  public static void setupLocal(JobConf jobConf, String jobName) {
    jobConf.set("fs.defaultFS", "local");
    jobConf.set("mapreduce.job.maps", "1");
    jobConf.set("mapreduce.job.reduces", "1");

    // Set a name of the Job
    jobConf.setJobName(jobName);
  }

  public static void setupInputOutput(
    JobConf jobConf,
    Class<?> outputKeyClass,
    Class<?> outputValueClass,
    Class<? extends Mapper> mapperClass,
    Class<? extends Reducer> reducerClass,
    Class<? extends TextInputFormat> inputFormatClass,
    Class<? extends TextOutputFormat> outputFormatClass
  ) {
    // Specify data type of output key and value
    jobConf.setOutputKeyClass(outputKeyClass);
    jobConf.setOutputValueClass(outputValueClass);

    // Specify names of Mapper and Reducer Class
    jobConf.setMapperClass(mapperClass);
    jobConf.setReducerClass(reducerClass);

    // Specify formats of the data type of input and output
    jobConf.setInputFormat(inputFormatClass);
    jobConf.setOutputFormat(outputFormatClass);
  }
}
