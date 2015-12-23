package com.jd.o2o.db4o.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.db4o.ObjectContainer;
import com.db4o.ObjectSet;
import com.db4o.ext.DatabaseClosedException;
import com.db4o.ext.DatabaseReadOnlyException;
import com.db4o.ext.Db4oException;
import com.db4o.ext.Db4oIOException;
import com.db4o.ext.ExtObjectContainer;
import com.db4o.query.Predicate;
import com.db4o.query.Query;
import com.db4o.query.QueryComparator;
import com.jd.o2o.db4o.exception.Db4oJdkException;

/**
 * 主从数据库容器
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月27日 上午10:51:58
 */
class MultipleObjectContainer implements ObjectContainer {
  private static Logger LOGGER = LoggerFactory.getLogger(MultipleObjectContainer.class);

  private final ObjectContainerBean masterObjectContainerBean;
  private final List<ObjectContainerBean> backupObjectContainerList =
      new ArrayList<ObjectContainerBean>();

  // 操作失败重试次数
  private final int reTryTimes;

  MultipleObjectContainer(ObjectContainerBean masterObjectContainerBean,
      List<ObjectContainerBean> backupObjectContainerList, int reTryTimes) {
    if (masterObjectContainerBean.db4oDataSource == null
        || masterObjectContainerBean.objectContainer == null) {
      throw new Db4oJdkException(
          "masterObjectContainerBean is illegal: " + masterObjectContainerBean);
    }
    this.masterObjectContainerBean = masterObjectContainerBean;
    if (backupObjectContainerList != null) {
      for (ObjectContainerBean bean : backupObjectContainerList) {
        if (bean == null || bean.db4oDataSource == null || bean.objectContainer == null) {
          continue;
        }
        this.backupObjectContainerList.add(bean);
      }
    }
    this.reTryTimes = reTryTimes;
  }

  @Override
  public void activate(Object obj, int depth) throws Db4oIOException, DatabaseClosedException {
    Db4oException exception = tryActivate(masterObjectContainerBean, obj, depth);
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      tryActivate(objectContainerBean, obj, depth);
    }
    if (exception != null) {
      throw exception;
    }
  }

  private Db4oException tryActivate(ObjectContainerBean objectContainerBean, Object obj,
      int depth) {
    int times = reTryTimes;
    Db4oException exception = null;
    do {
      try {
        if (objectContainerBean.objectContainer != null) {
          objectContainerBean.objectContainer.activate(obj, depth);
        }
        return null;
      } catch (DatabaseClosedException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " obj: " + obj + " depth: " + depth
            + " activate error: ", exception);
        ObjectContainer neonateObjectContainer = objectContainerBean.db4oDataSource.openClient();
        if (neonateObjectContainer == null) {
          break;
        }
        objectContainerBean.objectContainer = neonateObjectContainer;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " obj: " + obj + " depth: " + depth
            + " activate error: ", exception);
      }
    } while (times-- > 0);
    return exception;
  }

  @Override
  public boolean close() throws Db4oIOException {
    boolean result = true;
    if (masterObjectContainerBean.objectContainer != null) {
      try {
        result = masterObjectContainerBean.objectContainer.close();
      } catch (Db4oException e) {
        LOGGER.error(
            masterObjectContainerBean.db4oDataSource + " masterObjectContainerBean close error: ",
            e);
        result = false;
      }
    }
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      ObjectContainer backupObjectContainer = objectContainerBean.objectContainer;
      try {
        backupObjectContainer.close();
      } catch (Db4oException e) {
        LOGGER.error(objectContainerBean.db4oDataSource + " close error: ", e);
      }
    }
    return result;
  }

  @Override
  public void commit() throws Db4oIOException, DatabaseClosedException, DatabaseReadOnlyException {
    if (masterObjectContainerBean.objectContainer == null) {
      throw new Db4oIOException(masterObjectContainerBean + " commit() error");
    }
    Db4oException exception = tryCommit(masterObjectContainerBean);
    if (exception != null) {
      throw exception;
    }
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      tryCommit(objectContainerBean);
    }
  }

  private Db4oException tryCommit(ObjectContainerBean objectContainerBean) {
    int times = reTryTimes;
    Db4oException exception = null;
    do {
      try {
        objectContainerBean.objectContainer.commit();
        return null;
      } catch (DatabaseClosedException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " commit error: ", exception);
        break;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " commit error: ", exception);
      }
    } while (times-- > 0);
    return exception;
  }

  @Override
  public void deactivate(Object obj, int depth) throws DatabaseClosedException {
    Db4oException exception = tryDeactivate(masterObjectContainerBean, obj, depth);
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      tryDeactivate(objectContainerBean, obj, depth);
    }
    if (exception != null) {
      throw exception;
    }
  }

  private Db4oException tryDeactivate(ObjectContainerBean objectContainerBean, Object obj,
      int depth) {
    int times = reTryTimes;
    Db4oException exception = null;
    do {
      try {
        if (objectContainerBean.objectContainer != null) {
          objectContainerBean.objectContainer.deactivate(obj, depth);
        }
        return null;
      } catch (DatabaseClosedException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " obj: " + obj + " depth: " + depth
            + " deactivate error: ", exception);
        ObjectContainer neonateObjectContainer = objectContainerBean.db4oDataSource.openClient();
        if (neonateObjectContainer == null) {
          break;
        }
        objectContainerBean.objectContainer = neonateObjectContainer;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " obj: " + obj + " depth: " + depth
            + " deactivate error: ", exception);
      }
    } while (times-- > 0);
    return exception;
  }

  @Override
  public void delete(Object obj)
      throws Db4oIOException, DatabaseClosedException, DatabaseReadOnlyException {
    if (masterObjectContainerBean.objectContainer == null) {
      throw new Db4oIOException(masterObjectContainerBean + " delete(Object) error");
    }
    Db4oException exception = tryDelete(masterObjectContainerBean, obj);
    if (exception != null) {
      throw exception;
    }
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      tryDelete(objectContainerBean, obj);
    }
  }

  private Db4oException tryDelete(ObjectContainerBean objectContainerBean, Object obj) {
    int times = reTryTimes;
    Db4oException exception = null;
    do {
      try {
        objectContainerBean.objectContainer.delete(obj);
        return null;
      } catch (DatabaseClosedException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " obj: " + obj + " delete error: ",
            exception);
        ObjectContainer neonateObjectContainer = objectContainerBean.db4oDataSource.openClient();
        if (neonateObjectContainer == null) {
          break;
        }
        objectContainerBean.objectContainer = neonateObjectContainer;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " obj: " + obj + " delete error: ",
            exception);
      }
    } while (times-- > 0);
    return exception;
  }

  @Override
  public ExtObjectContainer ext() {
    // not supported yet
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> ObjectSet<T> queryByExample(Object template)
      throws Db4oIOException, DatabaseClosedException {
    ObjectSet<T> objSet = null;
    Db4oException exception = null;
    ObjectContainer masterObjectContainer = masterObjectContainerBean.objectContainer;
    if (masterObjectContainer == null) {
      exception = new Db4oIOException(masterObjectContainerBean + " queryByExample(Object) error");
    } else {
      try {
        objSet = masterObjectContainer.queryByExample(template);
        return objSet;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(masterObjectContainerBean.db4oDataSource + " template: " + template
            + " queryByExample error: ", exception);
      }
    }
    // 查从
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      ObjectContainer backupObjectContainer = objectContainerBean.objectContainer;
      try {
        objSet = backupObjectContainer.queryByExample(template);
        return objSet;
      } catch (Db4oException ex) {
        LOGGER.error(objectContainerBean.db4oDataSource + " template: " + template
            + " queryByExample error: ", ex);
      }
    }
    throw exception;
  }

  @Override
  public Query query() throws DatabaseClosedException {
    QueryProxy queryProxy = null;
    Query query = null;
    Db4oException exception = null;
    ObjectContainer masterObjectContainer = masterObjectContainerBean.objectContainer;
    if (masterObjectContainer == null) {
      exception = new Db4oIOException(masterObjectContainerBean + " query() error");
    } else {
      try {
        query = masterObjectContainer.query();
        queryProxy = new QueryProxy(query, backupObjectContainerList);
        return queryProxy;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(masterObjectContainerBean.db4oDataSource + " query error: ", exception);
      }
    }
    // 查从
    for (int i = 0, size = backupObjectContainerList.size(); i < size; i++) {
      ObjectContainerBean objectContainerBean = backupObjectContainerList.get(i);
      if (objectContainerBean != null) {
        ObjectContainer backupObjectContainer = objectContainerBean.objectContainer;
        try {
          query = backupObjectContainer.query();
          List<ObjectContainerBean> partBackupObjectContainerList =
              new ArrayList<ObjectContainerBean>();
          for (int j = i + 1; j < size; j++) {
            partBackupObjectContainerList.add(backupObjectContainerList.get(j));
          }
          queryProxy = new QueryProxy(query, partBackupObjectContainerList);
          return queryProxy;
        } catch (Db4oException ex) {
          LOGGER.error(objectContainerBean.db4oDataSource + " query error: ", ex);
        }
      }
    }
    throw exception;
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Class<TargetType> clazz)
      throws Db4oIOException, DatabaseClosedException {
    ObjectSet<TargetType> objectSet = null;
    Db4oException exception = null;
    ObjectContainer masterObjectContainer = masterObjectContainerBean.objectContainer;
    if (masterObjectContainer == null) {
      exception = new Db4oIOException(masterObjectContainerBean + " query(Class) error");
    } else {
      try {
        objectSet = masterObjectContainer.query(clazz);
        return objectSet;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(
            masterObjectContainerBean.db4oDataSource + " clazz: " + clazz + " query error: ",
            exception);
      }
    }
    // 查从
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      if (objectContainerBean != null) {
        ObjectContainer backupObjectContainer = objectContainerBean.objectContainer;
        try {
          objectSet = backupObjectContainer.query(clazz);
          return objectSet;
        } catch (Db4oException ex) {
          LOGGER.error(objectContainerBean.db4oDataSource + " clazz: " + clazz + " query error: ",
              ex);
        }
      }
    }
    throw exception;
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Predicate<TargetType> predicate)
      throws Db4oIOException, DatabaseClosedException {
    ObjectSet<TargetType> objectSet = null;
    Db4oException exception = null;
    ObjectContainer masterObjectContainer = masterObjectContainerBean.objectContainer;
    if (masterObjectContainer == null) {
      exception = new Db4oIOException(masterObjectContainerBean + " query(Predicate) error");
    } else {
      try {
        objectSet = masterObjectContainer.query(predicate);
        return objectSet;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(masterObjectContainerBean.db4oDataSource + " predicate: " + predicate
            + " query error: ", exception);
      }
    }
    // 查从
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      if (objectContainerBean != null) {
        ObjectContainer backupObjectContainer = objectContainerBean.objectContainer;
        try {
          objectSet = backupObjectContainer.query(predicate);
          return objectSet;
        } catch (Db4oException ex) {
          LOGGER.error(
              objectContainerBean.db4oDataSource + " predicate: " + predicate + " query error: ",
              ex);
        }
      }
    }
    throw exception;
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Predicate<TargetType> predicate,
      QueryComparator<TargetType> comparator) throws Db4oIOException, DatabaseClosedException {
    ObjectSet<TargetType> objectSet = null;
    Db4oException exception = null;
    ObjectContainer masterObjectContainer = masterObjectContainerBean.objectContainer;
    if (masterObjectContainer == null) {
      exception = new Db4oIOException(
          masterObjectContainerBean + " query(Predicate, QueryComparator) error");
    } else {
      try {
        objectSet = masterObjectContainer.query(predicate, comparator);
        return objectSet;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(masterObjectContainerBean.db4oDataSource + " predicate: " + predicate
            + " QueryComparator: " + comparator + " query error: ", exception);
      }
    }
    // 查从
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      if (objectContainerBean != null) {
        ObjectContainer backupObjectContainer = objectContainerBean.objectContainer;
        try {
          objectSet = backupObjectContainer.query(predicate, comparator);
          return objectSet;
        } catch (Db4oException ex) {
          LOGGER.error(objectContainerBean.db4oDataSource + " predicate: " + predicate
              + " QueryComparator: " + comparator + " query error: ", ex);
        }
      }
    }
    throw exception;
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Predicate<TargetType> predicate,
      Comparator<TargetType> comparator) throws Db4oIOException, DatabaseClosedException {
    ObjectSet<TargetType> objectSet = null;
    Db4oException exception = null;
    ObjectContainer masterObjectContainer = masterObjectContainerBean.objectContainer;
    if (masterObjectContainer == null) {
      exception =
          new Db4oIOException(masterObjectContainerBean + " query(Predicate, Comparator) error");
    } else {
      try {
        objectSet = masterObjectContainer.query(predicate, comparator);
        return objectSet;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(masterObjectContainerBean.db4oDataSource + " predicate: " + predicate
            + " comparator: " + comparator + " query error: ", exception);
      }
    }
    // 查从
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      if (objectContainerBean != null) {
        ObjectContainer backupObjectContainer = objectContainerBean.objectContainer;
        try {
          objectSet = backupObjectContainer.query(predicate, comparator);
          return objectSet;
        } catch (Db4oException ex) {
          LOGGER.error(objectContainerBean.db4oDataSource + " predicate: " + predicate
              + " comparator: " + comparator + " query error: ", ex);
        }
      }
    }
    throw exception;
  }

  @Override
  public void rollback()
      throws Db4oIOException, DatabaseClosedException, DatabaseReadOnlyException {
    Db4oException exception = tryRollback(masterObjectContainerBean);
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      tryRollback(objectContainerBean);
    }
    if (exception != null) {
      throw exception;
    }
  }

  private Db4oException tryRollback(ObjectContainerBean objectContainerBean) {
    int times = reTryTimes;
    Db4oException exception = null;
    do {
      try {
        if (objectContainerBean.objectContainer == null) {
          objectContainerBean.objectContainer.rollback();
        }
        return null;
      } catch (DatabaseClosedException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " rollback error: ", exception);
        break;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " rollback error: ", exception);
      }
    } while (times-- > 0);
    return exception;
  }

  @Override
  public void store(Object obj) throws DatabaseClosedException, DatabaseReadOnlyException {
    if (masterObjectContainerBean.objectContainer == null) {
      throw new Db4oIOException(masterObjectContainerBean + " store(Object) error");
    }
    Db4oException exception = tryStore(masterObjectContainerBean, obj);
    if (exception != null) {
      throw exception;
    }
    for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
      tryStore(objectContainerBean, obj);
    }
  }

  private Db4oException tryStore(ObjectContainerBean objectContainerBean, Object obj) {
    int times = reTryTimes;
    Db4oException exception = null;
    do {
      try {
        objectContainerBean.objectContainer.store(obj);
        return null;
      } catch (DatabaseClosedException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " obj: " + obj + " store error: ",
            exception);
        ObjectContainer neonateObjectContainer = objectContainerBean.db4oDataSource.openClient();
        if (neonateObjectContainer == null) {
          break;
        }
        objectContainerBean.objectContainer = neonateObjectContainer;
      } catch (Db4oException e) {
        exception = e;
        LOGGER.error(objectContainerBean.db4oDataSource + " obj: " + obj + " store error: ",
            exception);
      }
    } while (times-- > 0);
    return exception;
  }

  /**
   * 只需校验主库连接是否有效
   */
  void tryMasterObjectContainerCommit()
      throws Db4oIOException, DatabaseClosedException, DatabaseReadOnlyException {
    ObjectContainer masterObjectContainer = masterObjectContainerBean.objectContainer;
    if (masterObjectContainer == null) {
      throw new Db4oIOException("masterObjectContainer is null");
    }
    masterObjectContainer.commit();
  }

  @Override
  public String toString() {
    return "MultipleObjectContainer [masterObjectContainerBean=" + masterObjectContainerBean
        + ", backupObjectContainerList=" + backupObjectContainerList + ", reTryTimes=" + reTryTimes
        + "]";
  }

  /**
   * Db4oDataSource和ObjectContainer信息
   *
   * @author xionghui
   * @email xionghui@jd.com
   * @date 2015年11月26日 下午8:04:31
   */
  static class ObjectContainerBean {
    Db4oDataSource db4oDataSource;
    ObjectContainer objectContainer;

    public ObjectContainerBean(Db4oDataSource db4oDataSource, ObjectContainer objectContainer) {
      this.db4oDataSource = db4oDataSource;
      this.objectContainer = objectContainer;
    }

    @Override
    public String toString() {
      return "ObjectContainerBean [db4oDataSource=" + db4oDataSource + ", objectContainer="
          + objectContainer + "]";
    }
  }
}
