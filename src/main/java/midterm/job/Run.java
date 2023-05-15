package midterm.job;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Run {
  public static void setupAndRun(JobClient jobClient, JobConf jobConf) {
    jobClient.setConf(jobConf);

    try {
      // run job
      JobClient.runJob(jobConf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
