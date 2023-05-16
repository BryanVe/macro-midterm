package midterm.p8;

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

// ¿Cuál es el salario mínimo en USD de los "Data Scientists" que trabajan en la
// empresa de tamaño "M" en Canadá y tienen una experiencia laboral de nivel "SE"?

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

    // -------------------------------------------------------------------------

    // Second job
    JobClient jobClient2 = new JobClient();

    // Create a configuration object for the job
    JobConf jobConf2 = new JobConf(Main.class);

    // Config to run locally
    Config.setupLocal(jobConf2, "salaries2");
    Config.setupInputOutput(
      jobConf2,
      Text.class,
      Text.class,
      CustomMapper2.class,
      CustomReducer2.class,
      TextInputFormat.class,
      TextOutputFormat.class
    );

    // Log second job
    Log.printArgs2(args, jobConf2);

    // Setup and run the job 2
    Run.setupAndRun(jobClient2, jobConf2);

    // -------------------------------------------------------------------------

    // Third job
    JobClient jobClient3 = new JobClient();

    // Create a configuration object for the job
    JobConf jobConf3 = new JobConf(Main.class);

    // Config to run locally
    Config.setupLocal(jobConf3, "salaries3");
    Config.setupInputOutput(
      jobConf3,
      Text.class,
      Text.class,
      CustomMapper3.class,
      CustomReducer3.class,
      TextInputFormat.class,
      TextOutputFormat.class
    );

    // Log third job
    Log.printArgs3(args, jobConf3);

    // Setup and run the job 3
    Run.setupAndRun(jobClient3, jobConf3);
  }
}
