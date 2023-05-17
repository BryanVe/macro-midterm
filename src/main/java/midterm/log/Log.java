package midterm.log;

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
    System.out.println("Args: " + Arrays.toString(args));

    FileInputFormat.setInputPaths(jobConf, new Path(args[1]));
    FileOutputFormat.setOutputPath(jobConf, new Path(args[2]));
  }

  public static void printArgs2(String[] args, JobConf jobConf) throws IOException {
    System.out.println("Args: " + Arrays.toString(args));

    FileInputFormat.setInputPaths(jobConf, new Path(args[2]));
    FileOutputFormat.setOutputPath(jobConf, new Path(args[3]));
  }

  public static void printArgs3(String[] args, JobConf jobConf) throws IOException {
    System.out.println("Args: " + Arrays.toString(args));

    FileInputFormat.setInputPaths(jobConf, new Path(args[3]));
    FileOutputFormat.setOutputPath(jobConf, new Path(args[4]));
  }
}
