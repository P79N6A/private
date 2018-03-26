package com.yurun.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yurun on 18/3/26.
 */
public class Log4jMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(Log4jMain.class);

  private static final Logger METRIC_LOGGER = LoggerFactory.getLogger("Metric");

  private static final Logger STREAMING_LOGGER = LoggerFactory.getLogger("Streaming");

  public static void main(String[] args) {
    LOGGER.info("test root log");

    METRIC_LOGGER.info("test metric log");
    METRIC_LOGGER.info("test metric log2");

    STREAMING_LOGGER.info("test streaming log");
  }

}
