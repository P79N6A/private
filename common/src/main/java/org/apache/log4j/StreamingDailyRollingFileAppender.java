package org.apache.log4j;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.log4j.helpers.LogLog;

/**
 * Created by yurun on 18/3/26.
 *
 * Used by Spark Streaming
 */
public class StreamingDailyRollingFileAppender extends DailyRollingFileAppender {

  protected int maxBackupIndex = 1;

  public void setMaxBackupIndex(int maxBackups) {
    this.maxBackupIndex = maxBackups;
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
      LogLog.error(logDir.getName() + " no files");

      return;
    }

    Arrays.sort(logFiles, Comparator.comparing(File::getName));

    for (int index = 0; index < logFiles.length - maxBackupIndex; index++) {
      logFiles[index].delete();
    }
  }

}
