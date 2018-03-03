/*
 * Copyright (c) 2015 yy.com. 
 *
 * All Rights Reserved.
 *
 * This program is the confidential and proprietary information of 
 * YY.INC. ("Confidential Information").  You shall not disclose such
 * Confidential Information and shall use it only in accordance with
 * the terms of the license agreement you entered into with yy.com.
 */
package com.github.benhaixiao.commons.redis.lock;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benhaixiao.commons.redis.JedisWrapper;

/**
 * 利用redis进行锁操作
 * @author xiaobenhai
 */
public class JedisLock {
	private static final Logger LOG = LoggerFactory.getLogger(JedisLock.class);
	private JedisWrapper writeJedis;
	public static final int LOCK_TIMEOUT = 40; //防止任务超时不释放锁
	private String keyPrefix; //key的前缀，如果不为空则在key前加上该前缀加下划线

	public boolean lock(String key,int expire){
        boolean result = false;
        try {
        	String wkey = wrapKey(key);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String t = Thread.currentThread().getName();
            long setnx = writeJedis.setnx(wkey, t + "-" + sdf.format(new Date()) + "_" + System.currentTimeMillis());
            result = setnx == 1 ? true : false;
            if(result) {
            	writeJedis.expire(wkey, expire == 0 ? LOCK_TIMEOUT : expire);
            } else {
                //锁超时错误处理
                String string = writeJedis.get(wkey);
                if(string != null && !"".equals(string.trim())) {
                    String[] split = string.split("_");
                    if(split.length >= 2) {
                        long lastLockTimeMillis = Long.valueOf(split[split.length-1]);
                        long now = System.currentTimeMillis();
                        long v = now - lastLockTimeMillis;
                        if(v > (expire + 1) * 1000L) {
                        	writeJedis.delete(wkey);
                        }
                    }
                }
            }
        } catch (Throwable e) {
            LOG.warn(e.getMessage(), e);
        }
        return result;
    
	}
	
	public void unlock(String key){
		writeJedis.delete(wrapKey(key));
	}
	
	private String wrapKey(String key){
		return keyPrefix == null ? key : keyPrefix + "_" + key;
	}
	
	public void setWriteJedis(JedisWrapper writeJedis) {
		this.writeJedis = writeJedis;
	}
	public void setKeyPrefix(String keyPrefix) {
		this.keyPrefix = keyPrefix;
	}
}
