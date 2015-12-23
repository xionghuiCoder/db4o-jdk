package com.jd.o2o.db4o.exception;

/**
 * db4o连接池状态异常
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月26日 下午4:17:18
 */
public class Db4oStateException extends RuntimeException {
  private static final long serialVersionUID = 7202391659691764311L;

  public Db4oStateException() {
    super();
  }

  public Db4oStateException(String msg) {
    super(msg);
  }

  public Db4oStateException(Throwable cause) {
    super(cause);
  }

  public Db4oStateException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
