package midterm.log;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

public class Log {
  public static void printArgs1(String[] args, JobConf jobConf) throws IOException {
    if (Dotenv.load().get("HADOOP_ENV").equals("local")) {
      Configuration c = new Configuration();
      String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

      System.out.println("Input: " + files[0]);
      System.out.println("Output: " + files[1]);

      FileInputFormat.setInputPaths(jobConf, new Path(files[0]));
      FileOutputFormat.setOutputPath(jobConf, new Path(files[1]));
    } else {
      System.out.println("Args: " + Arrays.toString(args));

      FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
      FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
    }
  }

  public static void printArgs2(String[] args, JobConf jobConf) throws IOException {
    if (Dotenv.load().get("HADOOP_ENV").equals("local")) {
      Configuration c = new Configuration();
      String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

      System.out.println("Input: " + files[1]);
      System.out.println("Output: " + files[2]);

      FileInputFormat.setInputPaths(jobConf, new Path(files[1]));
      FileOutputFormat.setOutputPath(jobConf, new Path(files[2]));
    } else {
      System.out.println("Args: " + Arrays.toString(args));

      FileInputFormat.setInputPaths(jobConf, new Path(args[1]));
      FileOutputFormat.setOutputPath(jobConf, new Path(args[2]));
    }
  }

  public static void printArgs3(String[] args, JobConf jobConf) throws IOException {
    if (Dotenv.load().get("HADOOP_ENV").equals("local")) {
      Configuration c = new Configuration();
      String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

      System.out.println("Input: " + files[2]);
      System.out.println("Output: " + files[3]);

      FileInputFormat.setInputPaths(jobConf, new Path(files[2]));
      FileOutputFormat.setOutputPath(jobConf, new Path(files[3]));
    } else {
      System.out.println("Args: " + Arrays.toString(args));

      FileInputFormat.setInputPaths(jobConf, new Path(args[2]));
      FileOutputFormat.setOutputPath(jobConf, new Path(args[3]));
    }
  }
}
