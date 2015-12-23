package com.jd.o2o.db4o.core;

import java.util.Comparator;

import com.db4o.ObjectContainer;
import com.db4o.ObjectSet;
import com.db4o.config.Configuration;
import com.db4o.ext.DatabaseClosedException;
import com.db4o.ext.DatabaseReadOnlyException;
import com.db4o.ext.Db4oDatabase;
import com.db4o.ext.Db4oIOException;
import com.db4o.ext.Db4oUUID;
import com.db4o.ext.ExtObjectContainer;
import com.db4o.ext.InvalidIDException;
import com.db4o.ext.ObjectInfo;
import com.db4o.ext.StoredClass;
import com.db4o.ext.SystemInfo;
import com.db4o.foundation.NotSupportedException;
import com.db4o.io.Storage;
import com.db4o.query.Predicate;
import com.db4o.query.Query;
import com.db4o.query.QueryComparator;
import com.db4o.reflect.ReflectClass;
import com.db4o.reflect.generic.GenericReflector;
import com.jd.o2o.db4o.exception.Db4oJdkException;

/**
 * ExtObjectContainer代理，可以获取Db4oDataSource信息
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月26日 下午8:35:05
 */
public class ExtObjectContainerProxy implements ExtObjectContainer {
  private Db4oDataSource db4oDataSource;
  private ExtObjectContainer extObjectContainer;

  public ExtObjectContainerProxy(Db4oDataSource db4oDataSource,
      ExtObjectContainer extObjectContainer) {
    if (db4oDataSource == null || extObjectContainer == null) {
      throw new Db4oJdkException("ExtObjectContainerProxy is illegal, db4oDataSource: "
          + db4oDataSource + " extObjectContainer: " + extObjectContainer);
    }
    this.db4oDataSource = db4oDataSource;
    this.extObjectContainer = extObjectContainer;
  }

  public Db4oDataSource getDb4oDataSource() {
    return db4oDataSource;
  }

  @Override
  public void activate(Object obj, int depth) throws Db4oIOException, DatabaseClosedException {
    extObjectContainer.activate(obj, depth);
  }

  @Override
  public boolean close() throws Db4oIOException {
    return extObjectContainer.close();
  }

  @Override
  public void commit() throws Db4oIOException, DatabaseClosedException, DatabaseReadOnlyException {
    extObjectContainer.commit();
  }

  @Override
  public void deactivate(Object obj, int depth) throws DatabaseClosedException {
    extObjectContainer.deactivate(obj, depth);
  }

  @Override
  public void delete(Object obj)
      throws Db4oIOException, DatabaseClosedException, DatabaseReadOnlyException {
    extObjectContainer.delete(obj);
  }

  @Override
  public ExtObjectContainer ext() {
    return extObjectContainer.ext();
  }

  @Override
  public <T> ObjectSet<T> queryByExample(Object template)
      throws Db4oIOException, DatabaseClosedException {
    return extObjectContainer.queryByExample(template);
  }

  @Override
  public Query query() throws DatabaseClosedException {
    return extObjectContainer.query();
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Class<TargetType> clazz)
      throws Db4oIOException, DatabaseClosedException {
    return extObjectContainer.query(clazz);
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Predicate<TargetType> predicate)
      throws Db4oIOException, DatabaseClosedException {
    return extObjectContainer.query(predicate);
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Predicate<TargetType> predicate,
      QueryComparator<TargetType> comparator) throws Db4oIOException, DatabaseClosedException {
    return extObjectContainer.query(predicate, comparator);
  }

  @Override
  public <TargetType> ObjectSet<TargetType> query(Predicate<TargetType> predicate,
      Comparator<TargetType> comparator) throws Db4oIOException, DatabaseClosedException {
    return extObjectContainer.query(predicate, comparator);
  }

  @Override
  public void rollback()
      throws Db4oIOException, DatabaseClosedException, DatabaseReadOnlyException {
    extObjectContainer.rollback();
  }

  @Override
  public void store(Object obj) throws DatabaseClosedException, DatabaseReadOnlyException {
    extObjectContainer.store(obj);
  }

  @Override
  public void activate(Object obj) throws Db4oIOException, DatabaseClosedException {
    extObjectContainer.activate(obj);
  }

  @Override
  public void deactivate(Object obj) {
    extObjectContainer.deactivate(obj);
  }

  @Override
  public void backup(String path)
      throws Db4oIOException, DatabaseClosedException, NotSupportedException {
    extObjectContainer.backup(path);
  }

  @Override
  public void backup(Storage targetStorage, String path)
      throws Db4oIOException, DatabaseClosedException, NotSupportedException {
    extObjectContainer.backup(targetStorage, path);
  }

  @Override
  public void bind(Object obj, long id) throws InvalidIDException, DatabaseClosedException {
    extObjectContainer.bind(obj, id);
  }

  @Override
  public Configuration configure() {
    return extObjectContainer.configure();
  }

  @Override
  public Object descend(Object obj, String[] path) {
    return extObjectContainer.descend(obj, path);
  }

  @Override
  public <T> T getByID(long ID) throws DatabaseClosedException, InvalidIDException {
    return extObjectContainer.getByID(ID);
  }

  @Override
  public <T> T getByUUID(Db4oUUID uuid) throws DatabaseClosedException, Db4oIOException {
    return extObjectContainer.getByUUID(uuid);
  }

  @Override
  public long getID(Object obj) {
    return extObjectContainer.getID(obj);
  }

  @Override
  public ObjectInfo getObjectInfo(Object obj) {
    return extObjectContainer.getObjectInfo(obj);
  }

  @Override
  public Db4oDatabase identity() {
    return extObjectContainer.identity();
  }

  @Override
  public boolean isActive(Object obj) {
    return extObjectContainer.isActive(obj);
  }

  @Override
  public boolean isCached(long ID) {
    return extObjectContainer.isCached(ID);
  }

  @Override
  public boolean isClosed() {
    return extObjectContainer.isClosed();
  }

  @Override
  public boolean isStored(Object obj) throws DatabaseClosedException {
    return extObjectContainer.isStored(obj);
  }

  @Override
  public ReflectClass[] knownClasses() {
    return extObjectContainer.knownClasses();
  }

  @Override
  public Object lock() {
    return extObjectContainer.lock();
  }

  @Override
  public ObjectContainer openSession() {
    return extObjectContainer.openSession();
  }

  @Override
  public <T> T peekPersisted(T object, int depth, boolean committed) {
    return extObjectContainer.peekPersisted(object, depth, committed);
  }

  @Override
  public void purge() {
    extObjectContainer.purge();
  }

  @Override
  public void purge(Object obj) {
    extObjectContainer.purge(obj);
  }

  @Override
  public GenericReflector reflector() {
    return extObjectContainer.reflector();
  }

  @Override
  public void refresh(Object obj, int depth) {
    extObjectContainer.refresh(obj, depth);
  }

  @Override
  public void releaseSemaphore(String name) {
    extObjectContainer.releaseSemaphore(name);
  }

  @Override
  public void store(Object obj, int depth) {
    extObjectContainer.store(obj, depth);
  }

  @Override
  public boolean setSemaphore(String name, int waitForAvailability) {
    return extObjectContainer.setSemaphore(name, waitForAvailability);
  }

  @Override
  public StoredClass storedClass(Object clazz) {
    return extObjectContainer.storedClass(clazz);
  }

  @Override
  public StoredClass[] storedClasses() {
    return extObjectContainer.storedClasses();
  }

  @Override
  public SystemInfo systemInfo() {
    return extObjectContainer.systemInfo();
  }

  @Override
  public long version() {
    return extObjectContainer.version();
  }

  @Override
  public String toString() {
    return "ObjectContainerBean [db4oDataSource=" + db4oDataSource + ", extObjectContainer="
        + extObjectContainer + "]";
  }
}
