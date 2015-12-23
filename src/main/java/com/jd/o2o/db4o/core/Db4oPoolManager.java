package com.jd.o2o.db4o.core;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * db4o连接池管理类
 *
 * @author caojunming
 * @date 2015年11月26日
 */
public class Db4oPoolManager {
  private final Lock poolLock = new ReentrantLock();
  private final Lock countLock = new ReentrantLock();

  // 接池关闭标志
  private volatile boolean isShutdown;

  // 连接池数量
  private final Map<Db4oDataSource, AtomicInteger> db4oPoolCount =
      new HashMap<Db4oDataSource, AtomicInteger>();

  // 连接池数据结构
  private final Map<Db4oDataSource, LinkedList<MultipleObjectContainerTimeBean>> db4oPool =
      new HashMap<Db4oDataSource, LinkedList<MultipleObjectContainerTimeBean>>();

  void increment(Db4oDataSource source) {
    countLock.lock();
    try {
      AtomicInteger count = db4oPoolCount.get(source);
      if (count == null) {
        count = new AtomicInteger(1);
        db4oPoolCount.put(source, count);
        return;
      }
      count.getAndIncrement();
    } finally {
      countLock.unlock();
    }
  }

  void decrement(Db4oDataSource source) {
    countLock.lock();
    try {
      AtomicInteger count = db4oPoolCount.get(source);
      if (count != null) {
        count.getAndDecrement();
      }
    } finally {
      countLock.unlock();
    }
  }

  /**
   * 检查并关闭空闲的连接
   */
  public void checkIdleConnection(long limitIdleTime, int initConn) {
    countLock.lock();
    try {
      Set<Db4oDataSource> db4oDataSourceSet = db4oPoolCount.keySet();
      if (db4oDataSourceSet == null) {
        return;
      }
      for (Db4oDataSource db4oDataSource : db4oDataSourceSet) {
        AtomicInteger countAtomic = db4oPoolCount.get(db4oDataSource);
        if (countAtomic == null) {
          continue;
        }
        while (true) {
          int count = countAtomic.get();
          int diff = count - initConn;
          if (diff <= 0) {
            break;
          }
          poolLock.lock();
          try {
            List<MultipleObjectContainerTimeBean> multipleObjectContainerTimeBeanList =
                db4oPool.get(db4oDataSource);
            if (multipleObjectContainerTimeBeanList == null
                || multipleObjectContainerTimeBeanList.size() == 0) {
              break;
            }
            MultipleObjectContainerTimeBean multipleObjectContainerTimeBean =
                multipleObjectContainerTimeBeanList
                    .get(multipleObjectContainerTimeBeanList.size() - 1);
            if (multipleObjectContainerTimeBean == null) {
              break;
            }
            long oldTime = multipleObjectContainerTimeBean.time;
            long nowTime = System.currentTimeMillis();
            if ((nowTime - oldTime) < limitIdleTime) {
              break;
            }
            MultipleObjectContainerTimeBean multipleObjectContainerBean =
                multipleObjectContainerTimeBeanList
                    .remove(multipleObjectContainerTimeBeanList.size() - 1);
            countAtomic.getAndDecrement();
            if (multipleObjectContainerBean != null) {
              MultipleObjectContainer multipleObjectContainer =
                  multipleObjectContainerBean.multipleObjectContainer;
              if (multipleObjectContainer != null) {
                multipleObjectContainer.close();
              }
            }
          } finally {
            poolLock.unlock();
          }
        }
      }
    } finally {
      countLock.unlock();
    }
  }

  /**
   * 进池
   */
  void entryPool(Db4oDataSource db4oDataSource, MultipleObjectContainer multipleObjectContainer) {
    poolLock.lock();
    try {
      MultipleObjectContainerTimeBean multipleObjectContainerTimeBean =
          new MultipleObjectContainerTimeBean(multipleObjectContainer);
      LinkedList<MultipleObjectContainerTimeBean> multipleObjectContainerTimeBeanList =
          db4oPool.get(db4oDataSource);
      if (multipleObjectContainerTimeBeanList == null) {
        multipleObjectContainerTimeBeanList = new LinkedList<MultipleObjectContainerTimeBean>();
        db4oPool.put(db4oDataSource, multipleObjectContainerTimeBeanList);
      }
      multipleObjectContainerTimeBeanList.addFirst(multipleObjectContainerTimeBean);
    } finally {
      poolLock.unlock();
    }
  }

  /**
   * 出池
   */
  MultipleObjectContainer exitPool(Db4oDataSource db4oDataSource) {
    MultipleObjectContainer multipleObjectContainer = null;
    poolLock.lock();
    try {
      List<MultipleObjectContainerTimeBean> multipleObjectContainerTimeBeanList =
          db4oPool.get(db4oDataSource);
      if (multipleObjectContainerTimeBeanList != null
          && multipleObjectContainerTimeBeanList.size() != 0) {
        MultipleObjectContainerTimeBean bean = multipleObjectContainerTimeBeanList.get(0);
        if (bean != null) {
          multipleObjectContainer = bean.multipleObjectContainer;
        }
        multipleObjectContainerTimeBeanList.remove(0);
      }
    } finally {
      poolLock.unlock();
    }
    return multipleObjectContainer;
  }

  /**
   * 关闭连接池
   */
  void shutdown() {
    poolLock.lock();
    try {
      isShutdown = true;
      db4oPool.clear();
    } finally {
      poolLock.unlock();
    }
  }

  /**
   * 检查连接池是否已经关闭
   */
  public boolean isShutdown() {
    return isShutdown;
  }

  /**
   * MultipleObjectContainer和当前时间
   */
  private static class MultipleObjectContainerTimeBean {
    private final MultipleObjectContainer multipleObjectContainer;
    private final long time = System.currentTimeMillis();

    private MultipleObjectContainerTimeBean(MultipleObjectContainer multipleObjectContainer) {
      this.multipleObjectContainer = multipleObjectContainer;
    }
  }
}
