package com.jd.o2o.db4o.core;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.db4o.ObjectContainer;
import com.db4o.ext.DatabaseClosedException;
import com.db4o.ext.Db4oIOException;
import com.jd.o2o.db4o.configuration.IConfiguration;
import com.jd.o2o.db4o.core.MultipleObjectContainer.ObjectContainerBean;
import com.jd.o2o.db4o.core.hook.IdleCheckHook;
import com.jd.o2o.db4o.exception.Db4oConfigException;
import com.jd.o2o.db4o.exception.Db4oJdkException;
import com.jd.o2o.db4o.exception.Db4oStateException;
import com.jd.o2o.db4o.util.MD5Util;

/**
 * Db4o连接池，也是Db4o门面，用于初始化db4o的一些配置的连接
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月26日 下午12:41:12
 */
public class Db4oPool implements IDb4oPool {
  private static Logger LOGGER = LoggerFactory.getLogger(Db4oPool.class);

  /**
   * 0:未初始化<br />
   * 1:初始化完成 <br />
   * -1:已关闭
   */
  public volatile int state = 0;

  private final Lock lock = new ReentrantLock();

  // 主库，一主
  private Db4oDataSource[] db4oDataSources;
  // 主的权重
  private int[] weights;
  // 从库，多从
  private Db4oDataSource[][] backupDb4oDataSources;

  // key为server, value为Db4oDataSource
  private final Map<String, Db4oDataSource> db4oDataSourceMap =
      new HashMap<String, Db4oDataSource>();

  // 根据该配置获取数据库信息和权重
  private IConfiguration configuration;

  // 主从对应关系：key为主, value为从（一主对多从）
  private final Map<Db4oDataSource, Db4oDataSource[]> servicebackupServicesMap =
      new HashMap<Db4oDataSource, Db4oDataSource[]>();

  // 一致性hash
  private final TreeMap<Long, String> consistentBuckets = new TreeMap<Long, String>();

  // 管理连接池
  private final Db4oPoolManager manager = new Db4oPoolManager();

  // 初始化的连接个数
  private int initConn;

  // 使用之前测试连接是否可用
  private boolean testBeforeUse;

  // 操作失败重试次数
  private int reTryTimes;

  /**
   * 大于initConn的空闲连接的回收时间(ms) <br />
   *
   * 零表示立即回收空闲连接 <br />
   * 负数表示不用回收空闲连接 <br />
   * 正数表示回收空闲limitIdleTime(ms)的连接
   */
  private long limitIdleTime = -1;

  public Db4oDataSource[] getDb4oDataSources() {
    return db4oDataSources;
  }

  public void setDb4oDataSources(Db4oDataSource[] db4oDataSources) {
    this.db4oDataSources = db4oDataSources;
  }

  public int[] getWeights() {
    return weights;
  }

  public void setWeights(int[] weights) {
    this.weights = weights;
  }

  public Db4oDataSource[][] getBackupDb4oDataSources() {
    return backupDb4oDataSources;
  }

  public void setBackupDb4oDataSources(Db4oDataSource[][] backupDb4oDataSources) {
    this.backupDb4oDataSources = backupDb4oDataSources;
  }

  public IConfiguration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(IConfiguration configuration) {
    this.configuration = configuration;
  }

  public int getInitConn() {
    return initConn;
  }

  public void setInitConn(int initConn) {
    if (initConn < 0) {
      throw new Db4oConfigException("initConn is negtive");
    }
    this.initConn = initConn;
  }

  public boolean isTestBeforeUse() {
    return testBeforeUse;
  }

  public void setTestBeforeUse(boolean testBeforeUse) {
    this.testBeforeUse = testBeforeUse;
  }

  public int getReTryTimes() {
    return reTryTimes;
  }

  public void setReTryTimes(int reTryTimes) {
    if (reTryTimes < 0) {
      throw new Db4oConfigException("reTryTimes is negtive");
    }
    this.reTryTimes = reTryTimes;
  }

  public long getLimitIdleTime() {
    return limitIdleTime;
  }

  public void setLimitIdleTime(long limitIdleTime) {
    this.limitIdleTime = limitIdleTime;
  }

  /**
   * 初始化
   */
  @Override
  public void init() {
    if (state != 0) {
      return;
    }
    long bg = System.currentTimeMillis();
    lock.lock();
    try {
      // double check
      if (state != 0) {
        return;
      }
      if (db4oDataSources == null) {
        if (configuration != null) {
          db4oDataSources = configuration.parseDb4oDataSources();
        }
        if (db4oDataSources == null) {
          throw new Db4oConfigException("db4oDataSource is null");
        }
      }
      for (Db4oDataSource db : db4oDataSources) {
        db4oDataSourceMap.put(db.getServer(), db);
      }
      if (weights == null && configuration != null) {
        weights = configuration.parseWeights();
      }
      if (weights != null && db4oDataSources.length != weights.length) {
        throw new Db4oConfigException("db4oDataSource and weights are differ");
      }
      if (backupDb4oDataSources == null && configuration != null) {
        backupDb4oDataSources = configuration.parseBackupDb4oDataSources();
      }
      if (backupDb4oDataSources != null) {
        if (db4oDataSources.length != backupDb4oDataSources.length) {
          throw new Db4oConfigException("db4oDataSource and backupDb4oDataSource are differ");
        }
        for (int i = 0, len = db4oDataSources.length; i < len; i++) {
          servicebackupServicesMap.put(db4oDataSources[i], backupDb4oDataSources[i]);
        }
      }

      // 初始化一致性桶和pool
      populateConsistentBuckets();

      if (limitIdleTime >= 0L) {
        IdleCheckHook startHook = IdleCheckHook.startHook(limitIdleTime, initConn, manager);
        startHook.await();
      }
      state = 1;
    } catch (InterruptedException e) {
      throw new Db4oJdkException(e);
    } finally {
      lock.unlock();
    }
    LOGGER.info("Db4oPool inited, cost {}ms", (System.currentTimeMillis() - bg));
  }

  /**
   * 初始化一致性桶和pool连接池(如果需要的话)
   */
  private void populateConsistentBuckets() {
    int totalWeight = 0, len = db4oDataSources.length;
    if (weights == null) {
      totalWeight = len;
    } else {
      for (int i = 0; i < weights.length; i++) {
        totalWeight += weights[i];
      }
    }
    for (int i = 0; i < len; i++) {
      String server = db4oDataSources[i].getServer();
      int weight = 1;
      if (weights != null) {
        weight = weights[i];
      }
      double factor = Math.floor(((double) (40 * len * weight)) / (double) totalWeight);
      for (long j = 0; j < factor; j++) {
        String serverJ = server + "-" + j;
        byte[] digest = MD5Util.md5(serverJ);
        for (int h = 0; h < 4; h++) {
          long k = //
              ((long) (digest[3 + h * 4] & 0xFF) << 24) //
                  | ((long) (digest[2 + h * 4] & 0xFF) << 16) //
                  | ((long) (digest[1 + h * 4] & 0xFF) << 8) //
                  | (digest[0 + h * 4] & 0xFF);
          consistentBuckets.put(k, server);
        }
      }
      // 初始化db4o连接池
      for (int j = 0; j < initConn; j++) {
        MultipleObjectContainer multipleObjectContainer = createObjectContainer(db4oDataSources[i]);
        manager.entryPool(db4oDataSources[i], multipleObjectContainer);
      }
    }
  }

  /**
   * 创建db4o连接容器(同时创建主从的)
   */
  MultipleObjectContainer createObjectContainer(Db4oDataSource db4oDataSource) {
    boolean createSuccess = false;
    ObjectContainer masterObjectContainer = db4oDataSource.openClient();
    if (masterObjectContainer != null) {
      createSuccess = true;
    }
    List<ObjectContainerBean> backupObjectContainerList = new ArrayList<ObjectContainerBean>();
    Db4oDataSource[] backupDb4oDataSources = servicebackupServicesMap.get(db4oDataSource);
    if (backupDb4oDataSources != null) {
      for (Db4oDataSource backupDb4oDataSource : backupDb4oDataSources) {
        if (backupDb4oDataSource == null) {
          continue;
        }
        // backup连不上不应该影响业务流程，默默吞掉异常
        ObjectContainer backupObjectContainer = backupDb4oDataSource.openClient();
        if (backupObjectContainer == null) {
          continue;
        }
        ObjectContainerBean objectContainerBean =
            new ObjectContainerBean(backupDb4oDataSource, backupObjectContainer);
        backupObjectContainerList.add(objectContainerBean);
        createSuccess = true;
      }
    }
    // 主从初始化连接都失败时退出
    if (!createSuccess) {
      throw new Db4oJdkException(db4oDataSource + " createObjectContainer error");
    }
    ObjectContainerBean masterObjectContainerBean =
        new ObjectContainerBean(db4oDataSource, masterObjectContainer);
    MultipleObjectContainer multipleObjectContainer = new MultipleObjectContainer(
        masterObjectContainerBean, backupObjectContainerList, reTryTimes);
    manager.increment(db4oDataSource);
    return multipleObjectContainer;
  }

  @Override
  public ObjectContainer getObjectContainer(String key) {
    checkState();
    if (consistentBuckets.size() == 0) {
      return null;
    }
    ObjectContainer objectContainer = null;
    // 只有一个server的话，直接获取
    if (consistentBuckets.size() == 4) {
      objectContainer = getConnection(consistentBuckets.get(consistentBuckets.firstKey()));
      return objectContainer;
    }
    String server = getBucket(key);
    objectContainer = getConnection(server);
    return objectContainer;
  }

  /**
   * 通过一致性hash获取key映射到的server
   */
  private String getBucket(String key) {
    long hash = MD5Util.md5HashingAlg(key);
    SortedMap<Long, String> tmap = consistentBuckets.tailMap(hash);
    boolean isEmpty = tmap.isEmpty();
    Long bucketKey = isEmpty ? consistentBuckets.firstKey() : tmap.firstKey();
    String server = isEmpty ? consistentBuckets.get(bucketKey) : tmap.get(bucketKey);
    return server;
  }

  @Override
  public ObjectContainer getConnection(String server) {
    checkState();
    Db4oDataSource db4oDataSource = db4oDataSourceMap.get(server);
    if (db4oDataSource == null) {
      return null;
    }
    MultipleObjectContainer multipleObjectContainer = manager.exitPool(db4oDataSource);
    if (multipleObjectContainer == null) {
      multipleObjectContainer = createObjectContainer(db4oDataSource);
    } else if (testBeforeUse) {
      // 校验连接是否可用(只有是旧连接是才需要test)
      multipleObjectContainer = testObjectContainer(multipleObjectContainer, db4oDataSource);
    }
    ObjectContainerProxy obProxy = null;
    if (multipleObjectContainer != null) {
      obProxy = new ObjectContainerProxy(this, manager, db4oDataSource, multipleObjectContainer);
    }
    return obProxy;
  }

  /**
   * 校验连接是否可用
   */
  private MultipleObjectContainer testObjectContainer(
      MultipleObjectContainer multipleObjectContainer, Db4oDataSource db4oDataSource) {
    if (multipleObjectContainer == null) {
      throw new Db4oJdkException("can't test ObjectContainer because it's null");
    }
    try {
      multipleObjectContainer.tryMasterObjectContainerCommit();
    } catch (DatabaseClosedException e) {
      try {
        multipleObjectContainer.close();
      } catch (Db4oIOException ex) {
        LOGGER.error("close multipleObjectContainer error: ", ex);
      }
      multipleObjectContainer = createObjectContainer(db4oDataSource);
    }
    return multipleObjectContainer;
  }

  /**
   * 检查是否已经初始化完成
   */
  private void checkState() {
    if (state != 1) {
      throw new Db4oStateException("db4o pool is not inited");
    }
  }

  @Override
  public void shutdown() {
    if (state != 1) {
      return;
    }
    state = -1;
    manager.shutdown();
  }
}
