package midterm.p7_2;

import midterm.config.Config;
import midterm.files.Folder;
import midterm.job.Run;
import midterm.log.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;

// ¿Cuál es el salario promedio en USD de un Principal Data Scientist en Alemania con 3 años de experiencia?

public class Main {
  public static void main(String[] args) throws IOException {
    Folder.deleteHDFSFolders();

    // First job
    JobClient jobClient = new JobClient();

    // Create a configuration object for the first job
    JobConf jobConf = new JobConf(Main.class);

    // Config to run locally
    Config.setupLocal(jobConf, "salaries");
    Config.setupInputOutput(
      jobConf,
      Text.class,
      Text.class,
      CustomMapper.class,
      CustomReducer.class,
      TextInputFormat.class,
      TextOutputFormat.class
    );

    // Log first job
    Log.printArgs1(args, jobConf);

    // Setup and run the job 2
    Run.setupAndRun(jobClient, jobConf);
  }
}
