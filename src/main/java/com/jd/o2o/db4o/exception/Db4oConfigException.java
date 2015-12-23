package com.jd.o2o.db4o.exception;

/**
 * Db4o配置错误
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月26日 下午12:40:33
 */
public class Db4oConfigException extends RuntimeException {
  private static final long serialVersionUID = -7708204606496000029L;

  public Db4oConfigException() {
    super();
  }

  public Db4oConfigException(String msg) {
    super(msg);
  }

  public Db4oConfigException(Throwable cause) {
    super(cause);
  }

  public Db4oConfigException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
