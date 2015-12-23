package com.jd.o2o.db4o.configuration.impl;

import java.io.Serializable;
import java.util.Arrays;

import com.jd.o2o.db4o.configuration.IConfiguration;
import com.jd.o2o.db4o.core.Db4oDataSource;
import com.jd.o2o.db4o.exception.Db4oConfigException;

/**
 * Db4oPool的一些基本配置
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月26日 下午5:05:43
 */
public class ConfigurationImpl implements IConfiguration, Serializable {
  private static final long serialVersionUID = 4648297799176847309L;

  private static final String SERVER_SPLIT = ",";

  private String servers;
  private String weights;
  private String[] backupServers;

  private String userName;
  private String passport;

  public String getServers() {
    return servers;
  }

  public void setServers(String servers) {
    this.servers = servers;
  }

  public String getWeights() {
    return weights;
  }

  public void setWeightStr(String weights) {
    this.weights = weights;
  }

  public String[] getBackupServers() {
    return backupServers;
  }

  public void setBackupServers(String[] backupServers) {
    this.backupServers = backupServers;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getPassport() {
    return passport;
  }

  public void setPassport(String passport) {
    this.passport = passport;
  }

  /**
   * 解析db4oDataSource
   */
  @Override
  public Db4oDataSource[] parseDb4oDataSources() {
    if (servers == null) {
      return null;
    }
    String[] serverArray = servers.split(SERVER_SPLIT);
    Db4oDataSource[] db4oDataSources = new Db4oDataSource[serverArray.length];
    for (int i = 0, len = serverArray.length; i < len; i++) {
      String server = serverArray[i];
      if (server == null) {
        continue;
      }
      db4oDataSources[i] = new Db4oDataSource(server, userName, passport);
    }
    return db4oDataSources;
  }

  /**
   * 解析weightStr
   */
  @Override
  public int[] parseWeights() {
    if (weights == null) {
      return null;
    }
    String[] weightStrs = weights.split(SERVER_SPLIT);
    int[] weights = new int[weightStrs.length];
    for (int i = 0, len = weightStrs.length; i < len; i++) {
      weights[i] = Integer.parseInt(weightStrs[i]);
    }
    return weights;
  }

  /**
   * 解析backupDb4oDataSource
   */
  @Override
  public Db4oDataSource[][] parseBackupDb4oDataSources() {
    if (backupServers == null || backupServers.length == 0) {
      return null;
    }
    int size = backupServers[0].split(SERVER_SPLIT).length;
    Db4oDataSource[][] backupDb4oDataSourceArray = new Db4oDataSource[size][];
    for (int i = 0, len = backupServers.length; i < len; i++) {
      String backupServerStr = backupServers[i];
      String[] backupServerArray = backupServerStr.split(SERVER_SPLIT);
      // 检查backupServers长度是否一致
      if (size != backupServerArray.length) {
        throw new Db4oConfigException("backupServers are differ");
      }
      for (int j = 0; j < size; j++) {
        String backupServer = backupServerArray[j];
        if (backupDb4oDataSourceArray[j] == null) {
          backupDb4oDataSourceArray[j] = new Db4oDataSource[len];
        }
        backupDb4oDataSourceArray[j][i] = new Db4oDataSource(backupServer, userName, passport);
      }
    }
    return backupDb4oDataSourceArray;
  }

  @Override
  public String toString() {
    return "ConfigurationImpl [servers=" + servers + ", weights=" + weights + ", backupServers="
        + Arrays.toString(backupServers) + ", userName=" + userName + ", passport=" + passport
        + "]";
  }
}
