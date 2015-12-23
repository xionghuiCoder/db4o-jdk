package com.jd.o2o.db4o.exception;

/**
 * Db4o-jdk全局异常类
 *
 * @author caojunming
 * @date 2015年11月25日
 */
public class Db4oJdkException extends RuntimeException {
  private static final long serialVersionUID = 7697519791390768174L;

  public Db4oJdkException() {
    super();
  }

  public Db4oJdkException(String msg) {
    super(msg);
  }

  public Db4oJdkException(Throwable cause) {
    super(cause);
  }

  public Db4oJdkException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
