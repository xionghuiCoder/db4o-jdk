package com.jd.o2o.db4o.core.hook;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jd.o2o.db4o.core.Db4oJdk;
import com.jd.o2o.db4o.core.Db4oPoolManager;
import com.jd.o2o.db4o.util.ThreadSleepUtil;

/**
 * 用于关闭超时的空闲连接, 注意: 只有在池中的连接个数大于initConn时才会回收
 *
 * @author caojunming
 * @date 2015年11月26日
 */
public class IdleCheckHook implements Runnable {
  private static Logger LOGGER = LoggerFactory.getLogger(Db4oJdk.class);

  private final long limitIdleTime;
  private final int initConn;
  private final Db4oPoolManager manager;
  private final CountDownLatch countDownLatch = new CountDownLatch(1);

  private IdleCheckHook(long limitIdleTime, int initConn, Db4oPoolManager manager) {
    this.limitIdleTime = limitIdleTime;
    this.initConn = initConn;
    this.manager = manager;
  }

  /**
   * Start IdleCheckHook
   */
  public static IdleCheckHook startHook(long limitIdleTime, int initConn, Db4oPoolManager manager) {
    IdleCheckHook idleCheckHook = new IdleCheckHook(limitIdleTime, initConn, manager);
    Thread thread = new Thread(idleCheckHook);
    thread.setName("db4o.idleCheckHook");
    thread.setDaemon(true);
    thread.start();
    return idleCheckHook;
  }

  @Override
  public void run() {
    // I'm running.
    LOGGER.info("db4o.IdleCheckHook running");
    countDownLatch.countDown();
    while (true) {
      try {
        if (manager.isShutdown()) {
          break;
        }
        manager.checkIdleConnection(limitIdleTime, initConn);
        ThreadSleepUtil.sleep();
      } catch (Exception e) {
        LOGGER.error("db4o.IdleCheckHook error: ", e);
      }
    }
    LOGGER.info("db4o.IdleCheckHook stop");
  }

  /**
   * 等待线程启动
   */
  public void await() throws InterruptedException {
    countDownLatch.await();
  }
}
