package com.jd.o2o.db4o.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * md5加密工具类
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月26日 下午1:42:06
 */
public class MD5Util {
  // avoid recurring construction
  private static ThreadLocal<MessageDigest> MD5 = new ThreadLocal<MessageDigest>() {
    @Override
    protected MessageDigest initialValue() {
      try {
        return MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException(e);
      }
    }
  };

  /**
   * md5加密inbuf
   */
  public static byte[] md5(String inbuf) {
    MessageDigest md5 = MD5.get();
    md5.reset();
    md5.update(inbuf.getBytes());
    // MD5的计算结果是一个 128位的长整数，
    byte[] digest = md5.digest();
    return digest;
  }

  /**
   * 获取md5 key后的hash值
   *
   * @param key
   * @return
   */
  public static long md5HashingAlg(String key) {
    byte[] digest = md5(key);
    long k = ((long) (digest[3] & 0xFF) << 24) //
        | ((long) (digest[2] & 0xFF) << 16) //
        | ((long) (digest[1] & 0xFF) << 8) //
        | (digest[0] & 0xFF);
    return k;
  }
}
