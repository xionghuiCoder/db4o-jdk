package com.jd.o2o.db4o.util;

/**
 * 释放CPU by 休眠一点时间
 *
 * @author caojunming
 * @date 2015年11月26日
 */
public class ThreadSleepUtil {
  private final static long MINUTE = 60 * 1000L;

  /**
   * 释放CPU: 如果不释放, CPU则一直忙碌
   */
  public static void sleep() {
    try {
      Thread.sleep(MINUTE);
    } catch (InterruptedException e) {
      // swallow the exception
    }
  }
}
