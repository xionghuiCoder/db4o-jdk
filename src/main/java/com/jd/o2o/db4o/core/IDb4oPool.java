package com.jd.o2o.db4o.core;

import com.db4o.ObjectContainer;

/**
 * 数据库池接口
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月26日 上午11:25:48
 */
public interface IDb4oPool {

  public void init();

  public ObjectContainer getObjectContainer(String key);

  public ObjectContainer getConnection(String server);

  public void shutdown();
}
