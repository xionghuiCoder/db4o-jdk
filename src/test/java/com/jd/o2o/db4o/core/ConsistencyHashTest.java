package com.jd.o2o.db4o.core;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import junit.framework.TestCase;

/**
 * 一致性hash测试
 *
 * @author xionghui
 * @email xionghui@jd.com
 * @date 2015年11月27日 上午11:59:33
 */
public class ConsistencyHashTest extends TestCase {
  private Db4oPool impl = new Db4oPool();

  private final static String USERNAME = "root";
  private final static String PASSPORT = "1";

  /**
   * 初始化Db4oPool
   */
  @Override
  public void setUp() {
    String ips =
        "172.16.156.88:2771,172.19.116.225:2771,172.19.116.226:2771,172.19.116.227:2771,172.19.116.228:2771,172.19.157.69:2771,172.19.157.70:2771,172.19.157.71:2771,172.19.157.72:2771,172.22.20.5:2771,172.22.20.6:2771,172.22.20.7:2771,172.22.20.8:2771,172.22.24.127:2771,172.22.24.128:2771,172.22.24.129:2771,172.22.24.130:2771,172.22.24.131:2771,172.22.24.132:2771";
    String[] ipArray = ips.split(",");
    Db4oDataSource[] db4oDataSources = new Db4oDataSource[ipArray.length];
    for (int i = 0, len = ipArray.length; i < len; i++) {
      db4oDataSources[i] = new Db4oDataSource(ipArray[i], USERNAME, PASSPORT);
    }
    impl.setDb4oDataSources(db4oDataSources);
    impl.init();
  }

  public void testConsistentBuckets() throws Exception {
    Field field = Db4oPool.class.getDeclaredField("consistentBuckets");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    TreeMap<Long, String> consistentBuckets = (TreeMap<Long, String>) field.get(impl);
    System.out.println(consistentBuckets.size() + " " + consistentBuckets);
    System.out.println();

    TreeMap<String, Long> consistentBucketsMap = new TreeMap<String, Long>();
    for (Map.Entry<Long, String> entry : consistentBuckets.entrySet()) {
      consistentBucketsMap.put(entry.getValue(), entry.getKey());
    }
    System.out.println(consistentBucketsMap.size() + " " + consistentBucketsMap);
    System.out.println();

    List<String> list = initRandomData();
    Map<String, Long> map = new HashMap<String, Long>();
    for (String s : list) {
      Method method = Db4oPool.class.getDeclaredMethod("getBucket", String.class);
      method.setAccessible(true);
      String server = (String) method.invoke(impl, s);
      Long count = map.get(server);
      if (count == null) {
        count = 0L;
      }
      count++;
      map.put(server, count);
    }
    System.out.println(map.size() + " " + map);
    System.out.println();

    Map<Long, String> countMap = new HashMap<Long, String>();
    for (Map.Entry<String, Long> entry : map.entrySet()) {
      countMap.put(entry.getValue(), entry.getKey());
    }
    System.out.println(countMap.size() + " " + countMap);
    System.out.println();
  }

  /**
   * 生成随机数据
   */
  private List<String> initRandomData() {
    List<String> list = new LinkedList<String>();
    for (int i = 0; i < 100000; i++) {
      String str = getRandomString(100);
      list.add(str);
    }
    return list;
  }

  /**
   * 生成随机字符串
   */
  private static String getRandomString(int length) { // length表示生成字符串的长度
    String base = "-abcdefghijklmnopqrstuvwxyz0123456789";
    Random random = new Random();
    StringBuilder sb = new StringBuilder();
    for (int i = 0, len = base.length(); i < length; i++) {
      int number = random.nextInt(len);
      sb.append(base.charAt(number));
    }
    return sb.toString();
  }
}
