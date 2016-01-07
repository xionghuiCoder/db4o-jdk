package com.jd.o2o.db4o.core;

import com.db4o.ObjectContainer;

/**
 * ObjectContainer初始化时配置
 * 
 * @author xionghui
 * @email xionghui@jd.com
 */
public interface IDb4oInitConfig {

  /**
   * 在创建完objectContainer后对其进行配置
   */
  public void configObjectContainer(ObjectContainer objectContainer);
}
