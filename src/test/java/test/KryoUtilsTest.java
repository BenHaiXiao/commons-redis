package test;

import com.alibaba.fastjson.JSON;
import com.github.benhaixiao.commons.serializer.fst.FstUtils;
import com.github.benhaixiao.commons.serializer.kryo.KryoPoolFactoryImpl;
import com.github.benhaixiao.commons.serializer.kryo.KryoUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * @author xiaobenhai
 */
public class KryoUtilsTest {
	private static KryoUtils kryoUtils = new KryoUtils();

	@Before
    public void before(){
        kryoUtils.setKryoPool(new KryoPoolFactoryImpl());
    }
     @Test
    public  void testKryoThreads() {
        // 一百个线程，每个线程序列化反序列化10万对象
        for (int i = 0; i < 100; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 1000; i++) {
                        // 序列化map
                        Map map = new HashMap();
                        map.put("threadname", Thread.currentThread().getName());
                        map.put("threadname2", new EntLiveInfo());
                        map.put("b", i);
                        byte[] bs = kryoUtils.kryoSerialize(map);
                        Object o = kryoUtils.kryoDeserialize(bs);
                        System.out.println(o);
                    }
                    System.out.println("Kryo,pool create size : " + kryoUtils.getCreateCount());
                }
            });
            t.start();
        }
    }
    @Test
    public  void testKryoMapD() {
        Map map = new HashMap();
        map.put("threadname", Thread.currentThread().getName());
        map.put("threadname2", new EntLiveInfo());
        map.put("i", 1);
        byte[] bs = kryoUtils.kryoSerialize(map);
        System.out.println(new String(bs));
        Long t = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            Object o = kryoUtils.kryoDeserialize(bs);
        }
        System.out.println("Kryo,反序列化Map对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms");
        System.out.println("Kryo,pool create size : " + kryoUtils.getCreateCount());
    }
    @Test
    public  void testFstMapD() {
        Map map = new HashMap();
        map.put("threadname", Thread.currentThread().getName());
        map.put("threadname2", new EntLiveInfo());
        map.put("i", 1);
        byte[] bs = FstUtils.fastSerialize(map);
        Long t = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            Object o = FstUtils.fastDeserialize(bs);
        }
        System.out.println("Fst ,反序列化Map对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms");
    }
    @Test
    public  void testJvmSerMapD() {
        byte[] bs = null;
        Map map = new HashMap();
        map.put("threadname", Thread.currentThread().getName());
        map.put("threadname2", new EntLiveInfo());
        map.put("i", 1);
        bs = SerializationUtils.serialize((Serializable) map);
        Long t = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            Object o = SerializationUtils.deserialize(bs);
        }
        System.out.println("Jvm ,反序列化Map对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms");
    }
    @Test
    public  void testKryoMap() {
        Map map = new HashMap();
        map.put("threadname", Thread.currentThread().getName());
        map.put("threadname2", new EntLiveInfo());
        Long t = System.currentTimeMillis();
        byte[] bs = null;
        for (int i = 0; i < 1000000; i++) {
            map.put("b", i);
            bs = kryoUtils.kryoSerialize(map);
        }
        System.out.println("Kryo,序列化Map对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms  序列化对象后大小：" + bytes2kb(bs.length * 1000000));
        System.out.println("Kryo,pool create size : " + kryoUtils.getCreateCount());
    }
    @Test
    public  void testKryoString() {
        Long t = System.currentTimeMillis();
        byte[] bs = null;
        for (int i = 0; i < 1000000; i++) {
            bs = kryoUtils.kryoSerialize("testeste");
        }
        System.out.println("Kryo,序列化Strg对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms  序列化对象后大小：" + bytes2kb(bs.length * 1000000));
        System.out.println("Kryo,pool create size : " + kryoUtils.getCreateCount());
    }

    public static void testJSONString() {
        Long t = System.currentTimeMillis();
        String bs = null;
        for (int i = 0; i < 1000000; i++) {
            bs = JSON.toJSONString("testeste");
        }
        System.out.println("JSON,序列化Strg对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms  序列化对象后大小：" + bytes2kb(bs.getBytes().length * 1000000));
        System.out.println("JSON,pool create size : " + kryoUtils.getCreateCount());
    }
    @Test
    public  void testKryoList() {
        EntLiveInfo e = new EntLiveInfo();
        List map = new ArrayList();
        map.add(e);
        Long t = System.currentTimeMillis();
        byte[] bs = null;
        for (int i = 0; i < 1000000; i++) {
            bs = kryoUtils.kryoSerialize(map);
        }
        System.out.println("Kryo,序列化List对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms  序列化对象后大小：" + bytes2kb(bs.length * 1000000));
        System.out.println("Kryo,pool create size : " + kryoUtils.getCreateCount());
    }
    @Test
    public  void testJSONList() {
        EntLiveInfo e = new EntLiveInfo();
        List map = new ArrayList();
        map.add(e);
        Long t = System.currentTimeMillis();
        String bs = null;
        for (int i = 0; i < 1000000; i++) {
            bs = JSON.toJSONString(map);
        }
        System.out.println("JSON,序列化List对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms  序列化对象后大小：" + bytes2kb(bs.getBytes().length * 1000000));
        System.out.println("JSON,pool create size : " + kryoUtils.getCreateCount());
    }
    @Test
    public  void testFstMap() {
        Map map = new HashMap();
        map.put("threadname", Thread.currentThread().getName());
        map.put("threadname2", new EntLiveInfo());
        Long t = System.currentTimeMillis();
        byte[] bs = null;
        for (int i = 0; i < 1000000; i++) {
            map.put("b", i);
            bs = FstUtils.fastSerialize(map);
        }
        System.out.println("Fst ,序列化Map对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms  序列化对象后大小：" + bytes2kb(bs.length * 1000000));
    }
    @Test
    public  void testJvmSerMap() {
        Map map = new HashMap();
        map.put("threadname", Thread.currentThread().getName());
        map.put("threadname2", new EntLiveInfo());
        Long t = System.currentTimeMillis();
        byte[] bs = null;
        for (int i = 0; i < 1000000; i++) {
            map.put("b", i);
            bs = SerializationUtils.serialize((Serializable) map);
        }
        System.out.println("Jvm ,序列化Map对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms  序列化对象后大小：" + bytes2kb(bs.length * 1000000));
    }
    @Test
    public  void testKryoD() {
        EntLiveInfo e = new EntLiveInfo();
        byte[] bs = kryoUtils.kryoSerialize(e);
        Long t = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            Object o = kryoUtils.kryoDeserialize(bs);
        }
        System.out.println("Kryo,反序列化EntLiveInfo对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms");
        System.out.println("Kryo,pool create size : " + kryoUtils.getCreateCount());
    }
    @Test
    public  void testFstD() {
        EntLiveInfo e = new EntLiveInfo();
        byte[] bs = FstUtils.fastSerialize(e);
        Long t = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            Object o = FstUtils.fastDeserialize(bs);
        }
        System.out.println("Fst ,反序列化EntLiveInfo对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms");
    }
    @Test
    public  void testJvmSerD() {
        byte[] bs = null;
        EntLiveInfo e = new EntLiveInfo();
        bs = SerializationUtils.serialize(e);
        Long t = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            Object o = SerializationUtils.deserialize(bs);
        }
        System.out.println("Jvm ,反序列化EntLiveInfo对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms");
    }
    @Test
    public  void testKryo() {
        EntLiveInfo e = new EntLiveInfo();
        Long t = System.currentTimeMillis();
        byte[] bs = null;
        for (int i = 0; i < 1000000; i++) {
            bs = kryoUtils.kryoSerialize(e);
        }
        System.out.println("Kryo,序列化EntLiveInfo对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms  序列化对象后大小：" + bytes2kb(bs.length * 1000000));
        System.out.println("Kryo,pool create size : " + kryoUtils.getCreateCount());
    }
    @Test
    public  void testJSON() {
        EntLiveInfo e = new EntLiveInfo();
        Long t = System.currentTimeMillis();
        String bs = null;
        for (int i = 0; i < 1000000; i++) {
            bs = JSON.toJSONString(e);
        }
        System.out.println("JSON,序列化EntLiveInfo对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms  序列化对象后大小：" + bytes2kb(bs.getBytes().length * 1000000));
        System.out.println("JSON,pool create size : " + kryoUtils.getCreateCount());
    }
    @Test
    public  void testFst() {
        EntLiveInfo e = new EntLiveInfo();
        Long t = System.currentTimeMillis();
        byte[] bs = null;
        for (int i = 0; i < 1000000; i++) {
            bs = FstUtils.fastSerialize(e);
        }
        System.out.println("Fst ,序列化EntLiveInfo对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms  序列化对象后大小：" + bytes2kb(bs.length * 1000000));
    }
    @Test
    public  void testJvmSer() {
        EntLiveInfo e = new EntLiveInfo();
        Long t = System.currentTimeMillis();
        byte[] bs = null;
        for (int i = 0; i < 1000000; i++) {
            bs = SerializationUtils.serialize(e);
        }
        System.out.println("Jvm ,序列化EntLiveInfo对象，100万次，总耗时：" + (System.currentTimeMillis() - t) + "ms  序列化对象后大小：" + bytes2kb(bs.length * 1000000));
    }
    @Test
    public  void test() {

        byte[] bs;
        Object o;

        Map map = new HashMap();
        map.put("threadname", Thread.currentThread().getName());
        map.put("b", 123);
        bs = kryoUtils.kryoSerialize(map);
        o = kryoUtils.kryoDeserialize(bs);
        System.out.println(o);
        System.out.println();

        // int
        int ii = 1;
        bs = kryoUtils.kryoSerialize(ii);
        o = kryoUtils.kryoDeserialize(bs);
        System.out.println(o);
        System.out.println("====================================");

        // object
        EntLiveInfo e = new EntLiveInfo();
        bs = kryoUtils.kryoSerialize(e);
        o = kryoUtils.kryoDeserialize(bs);
        System.out.println("size:" + bs.length + o);
        System.out.println("====================================");

        // list
        List<EntLiveInfo> es = new ArrayList<EntLiveInfo>();
        es.add(new EntLiveInfo());
        es.add(new EntLiveInfo());
        bs = kryoUtils.kryoSerialize(es);
        o = kryoUtils.kryoDeserialize(bs);
        System.out.println(es);
        System.out.println("====================================");

        // map<String,Object>
        Map<String, EntLiveInfo> map2 = new HashMap<String, EntLiveInfo>();
        map2.put("1", new EntLiveInfo());
        bs = kryoUtils.kryoSerialize(map2);
        o = kryoUtils.kryoDeserialize(bs);
        System.out.println(o);
        Map map3 = (Map<String, EntLiveInfo>) o;
        System.out.println(map3.size() + " __ " + ((EntLiveInfo) map3.get("1")).toString());
        System.out.println("====================================");
    }

    public static String bytes2kb(long bytes) {
        BigDecimal filesize = new BigDecimal(bytes);
        BigDecimal megabyte = new BigDecimal(1024 * 1024);
        float returnValue = filesize.divide(megabyte, 2, BigDecimal.ROUND_UP).floatValue();
        if (returnValue > 1)
            return (returnValue + "MB");
        BigDecimal kilobyte = new BigDecimal(1024);
        returnValue = filesize.divide(kilobyte, 2, BigDecimal.ROUND_UP).floatValue();
        return (returnValue + "KB");
    }
}
