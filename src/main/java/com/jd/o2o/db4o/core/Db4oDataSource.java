package com.jd.o2o.db4o.core;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.db4o.ObjectContainer;
import com.db4o.cs.Db4oClientServer;
import com.db4o.ext.Db4oException;

/**
 * db4o数据源，用于存储ip、port和用户名密码
 * 
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月26日 下午7:34:28
 */
public class Db4oDataSource implements Serializable {
  private static Logger LOGGER = LoggerFactory.getLogger(Db4oDataSource.class);

  private static final long serialVersionUID = -3652962670844800468L;

  private static final String IPPORT_SPLIT = ":";

  private String server;
  private String ip;
  private int port;

  private String userName;
  private String passport;

  // ObjectContainer初始化时配置
  private IDb4oInitConfig db4oInitConfig;

  public Db4oDataSource() {}

  public Db4oDataSource(String server, String userName, String passport,
      IDb4oInitConfig db4oInitConfig) {
    this.server = server;
    String[] ipPort = server.split(IPPORT_SPLIT);
    this.ip = ipPort[0];
    this.port = Integer.parseInt(ipPort[1]);
    this.userName = userName;
    this.passport = passport;
    this.db4oInitConfig = db4oInitConfig;
  }

  public Db4oDataSource(String server, String ip, int port, String userName, String passport,
      IDb4oInitConfig db4oInitConfig) {
    this.server = server;
    this.ip = ip;
    this.port = port;
    this.userName = userName;
    this.passport = passport;
    this.db4oInitConfig = db4oInitConfig;
  }

  public String getServer() {
    return this.server;
  }

  public void setServer(String server) {
    this.server = server;
    String[] ipPort = server.split(":");
    this.ip = ipPort[0];
    this.port = Integer.parseInt(ipPort[1]);
  }

  public String getUserName() {
    return this.userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getPassport() {
    return this.passport;
  }

  public void setPassport(String passport) {
    this.passport = passport;
  }

  public ObjectContainer openClient() {
    ObjectContainer objectContainer = null;
    try {
      objectContainer =
          Db4oClientServer.openClient(this.ip, this.port, this.userName, this.passport);
    } catch (Db4oException e) {
      LOGGER.error(this + " openClient() error: ", e);
    }
    if (this.db4oInitConfig != null && objectContainer != null) {
      this.db4oInitConfig.configObjectContainer(objectContainer);
    }
    return objectContainer;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }

    if (!(object instanceof Db4oDataSource)) {
      return false;
    }

    Db4oDataSource ds = (Db4oDataSource) object;
    return this.server == null ? ds.server == null : this.server.equals(ds.server);
  }


  @Override
  public int hashCode() {
    return this.server == null ? 0 : this.server.hashCode();
  }

  @Override
  public String toString() {
    return "Db4oDataSource [server=" + this.server + ", ip=" + this.ip + ", port=" + this.port
        + ", userName=" + this.userName + ", passport=" + this.passport + "]";
  }
}
