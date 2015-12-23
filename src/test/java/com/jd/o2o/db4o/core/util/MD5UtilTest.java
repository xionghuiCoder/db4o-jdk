package com.jd.o2o.db4o.core.util;

import java.util.Arrays;

import com.jd.o2o.db4o.util.MD5Util;

import junit.framework.TestCase;

/**
 * md5加密工具类测试类
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月27日 上午11:48:36
 */
public class MD5UtilTest extends TestCase {

  /**
   * 测试md5获取digest的长度
   */
  public void testMd5() {
    byte[] digest = MD5Util.md5("");
    System.out.println(Arrays.toString(digest));
    digest = MD5Util.md5("s");
    System.out.println(Arrays.toString(digest));
    digest = MD5Util.md5("asdssadsadadsafdsfsafsdafdasdsadsadasdadsadadaseffsdf");
    System.out.println(Arrays.toString(digest));
  }
}
