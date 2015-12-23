package com.jd.o2o.db4o.configuration;

import com.jd.o2o.db4o.core.Db4oDataSource;

/**
 * 配置接口，可以自定义以提供更友好的规则
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月26日 下午7:08:46
 */
public interface IConfiguration {
  public Db4oDataSource[] parseDb4oDataSources();

  public int[] parseWeights();

  public Db4oDataSource[][] parseBackupDb4oDataSources();
}
