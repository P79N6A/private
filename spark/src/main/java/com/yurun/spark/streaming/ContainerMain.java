package com.yurun.spark.streaming;

import com.google.common.base.Splitter;
import java.util.Iterator;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * Created by yurun on 18/3/27.
 */
public class ContainerMain {

  public static final long CONTAINER_ID_BITMASK = 0xffffffffffL;
  private static final Splitter _SPLITTER = Splitter.on('_').trimResults();
  private static final String CONTAINER_PREFIX = "container";
  private static final String EPOCH_PREFIX = "e";

  private static ApplicationAttemptId toApplicationAttemptId(
      Iterator<String> it) throws NumberFormatException {
    return toApplicationAttemptId(Long.parseLong(it.next()), it);
  }

  private static ApplicationAttemptId toApplicationAttemptId(
      long clusterTimestamp, Iterator<String> it) throws NumberFormatException {
    ApplicationId appId = ApplicationId.newInstance(clusterTimestamp,
        Integer.parseInt(it.next()));
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, Integer.parseInt(it.next()));
    return appAttemptId;
  }

  public static ContainerId fromString(String containerIdStr) {
    Iterator<String> it = _SPLITTER.split(containerIdStr).iterator();
    if (!it.next().equals(CONTAINER_PREFIX)) {
      throw new IllegalArgumentException("Invalid ContainerId prefix: "
          + containerIdStr);
    }
    try {
      String epochOrClusterTimestampStr = it.next();
      long epoch = 0;
      ApplicationAttemptId appAttemptID = null;
      if (epochOrClusterTimestampStr.startsWith(EPOCH_PREFIX)) {
        String epochStr = epochOrClusterTimestampStr;
        System.out.println("epochStr: " + epochStr);
        epoch = Integer.parseInt(epochStr.substring(EPOCH_PREFIX.length()));
        appAttemptID = toApplicationAttemptId(it);
      } else {
        String clusterTimestampStr = epochOrClusterTimestampStr;
        long clusterTimestamp = Long.parseLong(clusterTimestampStr);
        appAttemptID = toApplicationAttemptId(clusterTimestamp, it);
      }
      long id = Long.parseLong(it.next());
      long cid = (epoch << 40) | id;
      ContainerId containerId = ContainerId.newContainerId(appAttemptID, cid);
      return containerId;
    } catch (NumberFormatException n) {
      throw new IllegalArgumentException("Invalid ContainerId: "
          + containerIdStr, n);
    }
  }

  public static void main(String[] args) {
    ContainerId containerId = fromString("container_e512_1520566051391_0577_01_000002");

    System.out.println(containerId.getContainerId());
    System.out.println(containerId.getApplicationAttemptId().getApplicationId().getId());
    System.out.println(containerId.getApplicationAttemptId().getAttemptId());
  }

}
