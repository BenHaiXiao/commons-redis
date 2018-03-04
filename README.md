# commons-redis
支持高可用、负载均衡、最佳读库选择、高效序列化的Redis Client。
## 背景
redis是业务开发过程中最常用分布式组件，如何实现redis的调用的高可用对业务研发来说至关重要,
一般的做法就是采用Redis集群，但是Redis集群使用存在如下问题：
1. 负载均衡，redis同mysql其他类型数据库一样存在IO问题，单机访问过大，IO负载高。
2. 高可用，redis client 其中一台宕机之后如何自动切换；
3. 高效， redis client 如何选择延时最小，使用率最高的 master或者slave进行连接；
4. 序列化方式， redis 高性能访问中，序列化成为了一大主要性能瓶颈；
4. 使用问题，业务方对原生的redisClient，如果要解决上述问题，需要自行扩展，难度较大，成本较高；
该项目主要解决redis client的上述问题

## 简介 
common-redis 是一个支持自定义配置，sentinel哨兵机制的高可用，负载均衡redis client，且使用方便操作简单。
主要支持功能点如下：
1.  哨兵模式， 支持sentinel 主从切换，自动选主功能；
2.  主备模式， 支持主备自动切换；
3.  读最少延时模式 ， 支持Redis client 自动选择连接 最小延迟的读库 （如redis读写分离情况下，选择最佳读库）。
4.  集成FST，Kryo，JavaSerializer序列化方式，默认avaSerializer, 解决了Redis序列化新能问题;
该项目已经在大公司生产环境落地使用。

## 使用方法

###1. jedis线程池配置

```
    <!--Jedis线程池配置 start-->
    <bean id="jedisPoolConfig" class="redis.clients.jedis.JedisPoolConfig">
        <property name="maxTotal" value="300"/>
        <property name="maxIdle" value="200"/>
        <property name="minIdle" value="50"/>
        <property name="maxWaitMillis" value="5000"/>
        <property name="testOnBorrow" value="true"/>
        <property name="testOnReturn" value="false"/>
         <property name="testWhileIdle" value="true" />
    </bean>
    <!--Jedis线程池配置 end-->
 ```

### 2. 哨兵模式配置 

```
    <!--哨兵模式  start -->
    <bean id="sentinelJedisPoolMaster" class="redis.clients.jedis.JedisSentinelPool">
        <constructor-arg name="masterName" value="${redis.master}"/>
        <constructor-arg name="sentinels">
            <set>
                <value>${redis.sentinel1.host}:${redis.sentinel1.port}</value>
                <value>${redis.sentinel2.host}:${redis.sentinel2.port}</value>
                <value>${redis.sentinel3.host}:${redis.sentinel3.port}</value>
            </set>
        </constructor-arg>
        <constructor-arg name="poolConfig" ref="jedisPoolConfig"/>
        <constructor-arg name="timeout" value="${redis.timeout}" type="int"/>
    </bean>

    <bean id="sentinelJedisPoolMgr" class="com.github.benhaixiao.commons.redis.JedisPoolHalbManager">
        <property name="jedisPools">
            <list>
                <ref bean="sentinelJedisPoolMaster"/>
            </list>
        </property>
    </bean>
    <!--哨兵模式  end-->

```


### 3. 主备模式配置 

```
 <!--主备模式  start-->
    <bean id="masterJedisPoolMaster" class="redis.clients.jedis.JedisPool">
        <constructor-arg name="poolConfig" ref="jedisPoolConfig" />
        <constructor-arg name="host" value="${redis.host1}" />
        <constructor-arg name="port" value="${redis.port1}" />
        <constructor-arg name="timeout" value="${redis.timeout}" />
    </bean>

    <bean id="slaveJedisPoolSlave" class="redis.clients.jedis.JedisPool">
        <constructor-arg name="poolConfig" ref="jedisPoolConfig" />
        <constructor-arg name="host" value="${redis.host2}" />
        <constructor-arg name="port" value="${redis.port2}" />
        <constructor-arg name="timeout" value="${redis.timeout}" />
    </bean>

    <bean id="readAndWriteJedisPoolManager" class="com.github.benhaixiao.commons.redis.JedisPoolHalbManager">
        <property name="jedisPools">
            <list>
                <ref bean="masterJedisPoolMaster" />
                <ref bean="slaveJedisPoolSlave" />
                <!--注意：支持多主，读写优先级顺序从上至下，建议按照优先级，排序-->
            </list>
        </property>
    </bean>
    <!--主备模式  end-->
```

### 4. 读最少延时模式配置 

```

    <!--读最少延时模式  start-->
    <bean id="minRRJedisPoolMgr" class="com.github.benhaixiao.commons.redis.JedisSlavePoolManager">
        <constructor-arg name="masterName" value="${redis.master}"/>
        <constructor-arg name="sentinels">
            <set>
                <value>${redis.sentinel1.host}:${redis.sentinel1.port}</value>
                <value>${redis.sentinel2.host}:${redis.sentinel2.port}</value>
                <value>${redis.sentinel3.host}:${redis.sentinel3.port}</value>
            </set>
        </constructor-arg>
        <constructor-arg name="poolConfig" ref="jedisPoolConfig"/>
        <constructor-arg name="timeout" value="${redis.timeout}" type="int"/>
    </bean>
    <!--读最少延时模式  end-->
```




