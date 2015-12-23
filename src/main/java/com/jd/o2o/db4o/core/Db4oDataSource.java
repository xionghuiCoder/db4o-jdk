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

  public Db4oDataSource() {}

  public Db4oDataSource(String server, String userName, String passport) {
    this.server = server;
    String[] ipPort = server.split(IPPORT_SPLIT);
    ip = ipPort[0];
    port = Integer.parseInt(ipPort[1]);
    this.userName = userName;
    this.passport = passport;
  }

  public Db4oDataSource(String server, String ip, int port, String userName, String passport) {
    this.server = server;
    this.ip = ip;
    this.port = port;
    this.userName = userName;
    this.passport = passport;
  }

  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
    String[] ipPort = server.split(":");
    ip = ipPort[0];
    port = Integer.parseInt(ipPort[1]);
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

  public ObjectContainer openClient() {
    ObjectContainer objectContainer = null;
    try {
      objectContainer = Db4oClientServer.openClient(ip, port, userName, passport);
    } catch (Db4oException e) {
      LOGGER.error(this + " openClient() error: ", e);
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
    return server == null ? ds.server == null : server.equals(ds.server);
  }


  @Override
  public int hashCode() {
    return server == null ? 0 : server.hashCode();
  }

  @Override
  public String toString() {
    return "Db4oDataSource [server=" + server + ", ip=" + ip + ", port=" + port + ", userName="
        + userName + ", passport=" + passport + "]";
  }
}
