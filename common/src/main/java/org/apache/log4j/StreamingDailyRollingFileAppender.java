package org.apache.log4j;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import org.apache.log4j.helpers.LogLog;
import org.apache.spark.SparkEnv;

/**
 * Created by yurun on 18/3/26.
 *
 * Enhanced DailyRollingFileAppender: support maxBackupIndex
 */
public class StreamingDailyRollingFileAppender extends DailyRollingFileAppender {

  protected int maxBackupIndex = 1;

  public void setMaxBackupIndex(int maxBackups) {
    this.maxBackupIndex = maxBackups;
  }

  @Override
  public void setFile(String file) {
    super.setFile(file);
  }

  @Override
  public void activateOptions() {
    super.activateOptions();

    SparkEnv sparkEnv = SparkEnv.get();

    if (Objects.nonNull(sparkEnv)) {
      String appName = sparkEnv.conf().get("spark.app.name");

      System.out.println(appName);
    } else {
      System.out.println("spark env is null");
    }
  }

  @Override
  void rollOver() throws IOException {
    super.rollOver();

    File logFile = new File(fileName);

    if (!logFile.exists()) {
      LogLog.error(fileName + " not exist");

      return;
    }

    File logDir = logFile.getParentFile();

    File[] logFiles = logDir.listFiles(
        (dir, filename) -> !filename.equals(logFile.getName())
            && filename.startsWith(logFile.getName()));

    if (logFiles == null || logFiles.length == 0) {
      return;
    }

    Arrays.sort(logFiles, Comparator.comparing(File::getName));

    for (int index = 0; index < logFiles.length - maxBackupIndex; index++) {
      logFiles[index].delete();
    }
  }

}
