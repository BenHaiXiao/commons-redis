package demo;

import com.github.benhaixiao.commons.redis.JedisWrapper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author xiaobenhai
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/application-context-jedis.xml")
public class SpringDemo {
   @Autowired
   private JedisWrapper writeJedisClient;
    @Autowired
    private JedisWrapper readJedisClient;


    @Test
    public void test(){
       String key = "testKey";
       String value = "testValue";
        writeJedisClient.set(key,value);
       Assert.assertEquals(value,readJedisClient.get(key));
   }
}
