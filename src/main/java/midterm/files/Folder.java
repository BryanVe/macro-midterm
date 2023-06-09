package midterm.files;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class Folder {
  public static boolean checkIfFolderExists(String path) {
    File file = new File(path);

    return file.exists();
  }

  public static void deleteFolder(String path) throws IOException {
    if (checkIfFolderExists(path))
      FileUtils.deleteDirectory(new File(path));
  }

  public static void deleteHDFSFolders() throws IOException {
    deleteFolder(System.getProperty("user.dir") + "/HDFS/output");
    deleteFolder(System.getProperty("user.dir") + "/HDFS/output2");
    deleteFolder(System.getProperty("user.dir") + "/HDFS/output3");
  }
}
