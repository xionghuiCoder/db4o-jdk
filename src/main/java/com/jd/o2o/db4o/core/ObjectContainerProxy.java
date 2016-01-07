package com.jd.o2o.db4o.core;

import java.util.Comparator;

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

/**
 * ObjectContainer代理类
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月26日 下午1:07:39
 */
public class ObjectContainerProxy implements ObjectContainer {
  private static Logger LOGGER = LoggerFactory.getLogger(ObjectContainerProxy.class);

  private Db4oJdk db4oPool;

  private Db4oPoolManager manager;

  private MultipleObjectContainer multipleObjectContainer;

  private Db4oDataSource db4oDataSource;

  // 标示是否是为未commit或者rollback的数据
  private boolean sign;

  public ObjectContainerProxy(Db4oJdk db4oPool, Db4oPoolManager manager,
      Db4oDataSource db4oDataSource, MultipleObjectContainer multipleObjectContainer) {
    this.db4oPool = db4oPool;
    this.manager = manager;
    this.db4oDataSource = db4oDataSource;
    this.multipleObjectContainer = multipleObjectContainer;
  }

  @Override
  public void activate(Object obj, int depth) throws Db4oIOException, DatabaseClosedException {
    multipleObjectContainer.activate(obj, depth);
  }

  @Override
  public boolean close() throws Db4oIOException {
    try {
      if (sign) {
        multipleObjectContainer.rollback();
      }
      multipleObjectContainer.tryMasterObjectContainerCommit();
    } catch (Db4oException e) {
      try {
        multipleObjectContainer.close();
      } catch (Db4oIOException ex) {
        LOGGER.error("close multipleObjectContainer error: ", ex);
      } finally {
        manager.decrement(db4oDataSource);
      }
      multipleObjectContainer = db4oPool.createObjectContainer(db4oDataSource);
    }
    manager.entryPool(db4oDataSource, multipleObjectContainer);
    return true;
  }

  @Override
  public void commit() throws Db4oIOException, DatabaseClosedException, DatabaseReadOnlyException {
    multipleObjectContainer.commit();
    sign = false;
  }

  @Override
  public void deactivate(Object obj, int depth) throws DatabaseClosedException {
    multipleObjectContainer.deactivate(obj, depth);
  }

  @Override
  public void delete(Object obj)
      throws Db4oIOException, DatabaseClosedException, DatabaseReadOnlyException {
    multipleObjectContainer.delete(obj);
    sign = true;
  }

  @Override
  public ExtObjectContainer ext() {
    return multipleObjectContainer.ext();
  }

  @Override
  public <T> ObjectSet<T> queryByExample(Object template)
      throws Db4oIOException, DatabaseClosedException {
    return multipleObjectContainer.queryByExample(template);
  }

  @Override
  public Query query() throws DatabaseClosedException {
    return multipleObjectContainer.query();
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Class<TargetType> clazz)
      throws Db4oIOException, DatabaseClosedException {
    return multipleObjectContainer.query(clazz);
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Predicate<TargetType> predicate)
      throws Db4oIOException, DatabaseClosedException {
    return multipleObjectContainer.query(predicate);
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Predicate<TargetType> predicate,
      QueryComparator<TargetType> comparator) throws Db4oIOException, DatabaseClosedException {
    return multipleObjectContainer.query(predicate, comparator);
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Predicate<TargetType> predicate,
      Comparator<TargetType> comparator) throws Db4oIOException, DatabaseClosedException {
    return multipleObjectContainer.query(predicate, comparator);
  }

  @Override
  public void rollback()
      throws Db4oIOException, DatabaseClosedException, DatabaseReadOnlyException {
    multipleObjectContainer.rollback();
    sign = false;
  }

  @Override
  public void store(Object obj) throws DatabaseClosedException, DatabaseReadOnlyException {
    multipleObjectContainer.store(obj);
    sign = true;
  }

  @Override
  public String toString() {
    return "ObjectContainerProxy [multipleObjectContainer=" + multipleObjectContainer
        + ", db4oDataSource=" + db4oDataSource + ", sign=" + sign + "]";
  }
}
