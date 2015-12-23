package com.jd.o2o.db4o.core;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.db4o.ObjectSet;
import com.db4o.ext.Db4oException;
import com.db4o.query.Constraint;
import com.db4o.query.Constraints;
import com.db4o.query.Query;
import com.db4o.query.QueryComparator;
import com.jd.o2o.db4o.core.MultipleObjectContainer.ObjectContainerBean;
import com.jd.o2o.db4o.exception.Db4oJdkException;

/**
 * 查询代理
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月27日 下午4:05:33
 */
public class QueryProxy implements Query {
  private static Logger LOGGER = LoggerFactory.getLogger(QueryProxy.class);

  /**
   * 记录操作步骤
   */
  private static final List<Operation> operationList = new CopyOnWriteArrayList<Operation>();

  /**
   * 记录操作对象
   */
  private static final List<Object> invokeObjList = new CopyOnWriteArrayList<Object>();

  private final Query query;

  private final List<ObjectContainerBean> backupObjectContainerList;

  private static final Method CONSTRAIN_METHOD;
  private static final Method CONSTRAINTS_METHOD;
  private static final Method DESCEND_METHOD;
  private static final Method ORDERASCENDING_METHOD;
  private static final Method ORDERDESCENDING_METHOD;
  private static final Method SORTBY_QUERYCOMPARATOR_METHOD;
  private static final Method SORTBY_COMPARATOR_METHOD;

  static {
    try {
      CONSTRAIN_METHOD = Query.class.getMethod("constrain", Object.class);
      CONSTRAINTS_METHOD = Query.class.getMethod("constraints");
      DESCEND_METHOD = Query.class.getMethod("descend", String.class);
      ORDERASCENDING_METHOD = Query.class.getMethod("orderAscending");
      ORDERDESCENDING_METHOD = Query.class.getMethod("orderDescending");
      SORTBY_QUERYCOMPARATOR_METHOD = Query.class.getMethod("sortBy", QueryComparator.class);
      SORTBY_COMPARATOR_METHOD = Query.class.getMethod("sortBy", Comparator.class);
    } catch (Exception e) {
      throw new Db4oJdkException(e);
    }
  }

  public QueryProxy(Query query, List<ObjectContainerBean> backupObjectContainerList) {
    this.query = query;
    addInvokeObj(query);
    this.backupObjectContainerList = backupObjectContainerList;
  }

  @Override
  public Constraint constrain(Object constraint) {
    trackOperation(query, CONSTRAIN_METHOD, constraint);
    Constraint constrain = query.constrain(constraint);
    Constraint constraintProxy = new ConstraintProxy(constrain, this);
    return constraintProxy;
  }

  @Override
  public Constraints constraints() {
    Object[] args = null;
    trackOperation(query, CONSTRAINTS_METHOD, args);
    Constraints constraints = query.constraints();
    Constraints constraintsProxy = new ConstraintsProxy(constraints, this);
    return constraintsProxy;
  }

  @Override
  public Query descend(String fieldName) {
    trackOperation(query, DESCEND_METHOD, fieldName);
    Query descendQuery = query.descend(fieldName);
    if (descendQuery.equals(query)) {
      return this;
    }
    QueryProxy queryProxy = new QueryProxy(descendQuery, backupObjectContainerList);
    return queryProxy;
  }

  @Override
  public <T> ObjectSet<T> execute() {
    ObjectSet<T> objectSet = null;
    Db4oException exception = null;
    Db4oJdkException retryException = null;
    try {
      objectSet = query.execute();
    } catch (Db4oException e) {
      exception = e;
      LOGGER.error(query + " execute error: ", e);
      for (ObjectContainerBean objectContainerBean : backupObjectContainerList) {
        if (objectContainerBean != null) {
          try {
            objectSet = retryExecute(objectContainerBean);
            retryException = null;
          } catch (Db4oJdkException ex) {
            retryException = ex;
            LOGGER.error(objectContainerBean.db4oDataSource + " retryExecute error: ", ex);
          }
        }
      }
    }
    // 清空操作记录
    operationList.clear();
    // 清空对象记录
    invokeObjList.clear();
    if (exception != null && retryException != null) {
      throw exception;
    }
    return objectSet;
  }

  /**
   * 使用从库的query来重新执行
   */
  private <T> ObjectSet<T> retryExecute(ObjectContainerBean objectContainerBean) {
    try {
      int len = invokeObjList.size();
      List<Object> backupInvokeList = new ArrayList<Object>();
      Query query = objectContainerBean.objectContainer.query();
      backupInvokeList.add(query);
      for (int i = 0, size = operationList.size(); i < size; i++) {
        Operation operation = operationList.get(i);
        Method method = operation.method;
        Object[] param = operation.param;
        if (param != null) {
          // 尝试替换Constraint、Constraints或者Query参数
          for (int j = 0, paramLen = param.length; j < paramLen; j++) {
            if ((param[j] instanceof Constraint) || (param[j] instanceof Query)) {
              for (int k = 0; k < len; k++) {
                if (k >= backupInvokeList.size()) {
                  break;
                }
                if (invokeObjList.get(k) == param[j]) {
                  param[j] = backupInvokeList.get(k);
                }
              }
            }
          }
        }
        Object invoke = null;
        for (int k = 0; k < len; k++) {
          if (k >= backupInvokeList.size()) {
            break;
          }
          if (invokeObjList.get(k) == operation.invokeObj) {
            invoke = backupInvokeList.get(k);
            break;
          }
        }
        if (invoke == null) {
          throw new Db4oJdkException("invoke is null");
        }
        Object result = method.invoke(invoke, param);
        if ((result instanceof Constraint || result instanceof Query) && result != invoke) {
          backupInvokeList.add(result);
        }
      }
      ObjectSet<T> objectSet = query.execute();
      return objectSet;
    } catch (Exception e) {
      throw new Db4oJdkException(e);
    }
  }

  @Override
  public Query orderAscending() {
    Object[] args = null;
    trackOperation(query, ORDERASCENDING_METHOD, args);
    Query orderAscendingQuery = query.orderAscending();
    if (orderAscendingQuery.equals(query)) {
      return this;
    }
    QueryProxy queryProxy = new QueryProxy(orderAscendingQuery, backupObjectContainerList);
    return queryProxy;
  }

  @Override
  public Query orderDescending() {
    Object[] args = null;
    trackOperation(query, ORDERDESCENDING_METHOD, args);
    Query orderDescendingQuery = query.orderDescending();
    if (orderDescendingQuery.equals(query)) {
      return this;
    }
    QueryProxy queryProxy = new QueryProxy(orderDescendingQuery, backupObjectContainerList);
    return queryProxy;
  }

  @Override
  public Query sortBy(QueryComparator<?> comparator) {
    trackOperation(query, SORTBY_QUERYCOMPARATOR_METHOD, comparator);
    Query sortByQuery = query.sortBy(comparator);
    if (sortByQuery.equals(query)) {
      return this;
    }
    QueryProxy queryProxy = new QueryProxy(sortByQuery, backupObjectContainerList);
    return queryProxy;
  }

  @Override
  public Query sortBy(@SuppressWarnings("rawtypes") Comparator comparator) {
    trackOperation(query, SORTBY_COMPARATOR_METHOD, comparator);
    Query sortByQuery = query.sortBy(comparator);
    if (sortByQuery.equals(query)) {
      return this;
    }
    QueryProxy queryProxy = new QueryProxy(sortByQuery, backupObjectContainerList);
    return queryProxy;
  }

  /**
   * 缓存invokeObj
   */
  void addInvokeObj(Object invokeObj) {
    invokeObjList.add(invokeObj);
  }

  /**
   * 记录操作步骤
   */
  void trackOperation(Object invokeObj, Method method, Object... args) {
    Operation operation = new Operation(invokeObj, method, args);
    operationList.add(operation);
  }

  /**
   * Query操作记录类
   */
  private static class Operation {
    private final Object invokeObj;
    private final Method method;
    private final Object[] param;

    private Operation(Object invokeObj, Method method, Object[] param) {
      this.invokeObj = invokeObj;
      this.method = method;
      this.param = param;
    }

    @Override
    public String toString() {
      return "Operation [method=" + method + ", param=" + param + "]";
    }
  }

  /**
   * 三种接口类型
   */
  static enum ClassType {
    Query, //
    Constraints, //
    Constraint, //
    ;
  }
}
