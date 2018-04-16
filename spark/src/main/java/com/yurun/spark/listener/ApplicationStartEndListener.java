package com.yurun.spark.listener;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationStartEndListener extends SparkListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationStartEndListener.class);

  private String appName;
  private int executors;
  private int cores;
  private String mems;
  private long startTime;
  private long endTime;

  public ApplicationStartEndListener(SparkConf conf) {
    appName = conf.get("spark.app.name");
    executors = conf.getInt("spark.executor.instances", 0);
    cores = conf.getInt("spark.executor.cores", 0);
    mems = conf.get("spark.executor.memory");
  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    startTime = System.currentTimeMillis();
  }

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    endTime = System.currentTimeMillis();

    LOGGER.info("appName: {}", appName);
    LOGGER.info("executors: {}", executors);
    LOGGER.info("cores: {}", cores);
    LOGGER.info("mems: {}", mems);

    LOGGER.info("consume time: {}", (endTime - startTime));
  }

}
