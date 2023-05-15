package midterm.p1;

import io.github.cdimascio.dotenv.Dotenv;
import midterm.files.Folder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

public class Main {
  public static void main(String[] args) throws IOException {
    Folder.deleteFolder(System.getProperty("user.dir") + "/HDFS/output");
    Folder.deleteFolder(System.getProperty("user.dir") + "/HDFS/output2");

    JobClient jobClient = new JobClient();

    // Create a configuration object for the job
    JobConf jobConf = new JobConf(Main.class);

    // Config to run locally
    jobConf.set("fs.defaultFS", "local");
    jobConf.set("mapreduce.job.maps", "1");
    jobConf.set("mapreduce.job.reduces", "1");

    // Set a name of the Job
    jobConf.setJobName("salaries");

    // Specify data type of output key and value
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);

    // Specify names of Mapper and Reducer Class
    jobConf.setMapperClass(CustomMapper.class);
    jobConf.setReducerClass(CustomReducer.class);

    // Specify formats of the data type of input and output
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setOutputFormat(TextOutputFormat.class);

    if (Dotenv.load().get("HADOOP_ENV").equals("local")) {
      Configuration c = new Configuration();
      String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

      System.out.println("Input: " + files[0]);
      System.out.println("Output: " + files[1]);

      FileInputFormat.setInputPaths(jobConf, new Path(files[0]));
      FileOutputFormat.setOutputPath(jobConf, new Path(files[1]));
    } else {
      System.out.println("Args: " + Arrays.toString(args));

      FileInputFormat.setInputPaths(jobConf, new Path(args[1]));
      FileOutputFormat.setOutputPath(jobConf, new Path(args[2]));
    }

    jobClient.setConf(jobConf);

    try {
      // run job
      JobClient.runJob(jobConf);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // aaaaaaaaaaaaaaaaaaaaa
    JobClient jobClient2 = new JobClient();

    // Create a configuration object for the job
    JobConf jobConf2 = new JobConf(Main.class);

    // Config to run locally
    jobConf2.set("fs.defaultFS", "local");
    jobConf2.set("mapreduce.job.maps", "1");
    jobConf2.set("mapreduce.job.reduces", "1");

    // Set a name of the Job
    jobConf2.setJobName("salaries2");

    // Specify data type of output key and value
    jobConf2.setOutputKeyClass(Text.class);
    jobConf2.setOutputValueClass(Text.class);

    // Specify names of Mapper and Reducer Class
    jobConf2.setMapperClass(CustomMapper2.class);
    jobConf2.setReducerClass(CustomReducer2.class);

    // Specify formats of the data type of input and output
    jobConf2.setInputFormat(TextInputFormat.class);
    jobConf2.setOutputFormat(TextOutputFormat.class);

    if (Dotenv.load().get("HADOOP_ENV").equals("local")) {
      Configuration c = new Configuration();
      String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

      System.out.println("Input: " + files[1]);
      System.out.println("Output: " + files[2]);

      FileInputFormat.setInputPaths(jobConf2, new Path(files[1]));
      FileOutputFormat.setOutputPath(jobConf2, new Path(files[2]));
    } else {
      System.out.println("Args: " + Arrays.toString(args));

      FileInputFormat.setInputPaths(jobConf2, new Path(args[1]));
      FileOutputFormat.setOutputPath(jobConf2, new Path(args[2]));
    }

    jobClient2.setConf(jobConf2);

    try {
      // run job
      JobClient.runJob(jobConf2);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
