/*
 * Copyright (c) 2014 yy.com. 
 *
 * All Rights Reserved.
 *
 * This program is the confidential and proprietary information of 
 * YY.INC. ("Confidential Information").  You shall not disclose such
 * Confidential Information and shall use it only in accordance with
 * the terms of the license agreement you entered into with yy.com.
 */
package com.github.benhaixiao.commons.redis.lock;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 适用在开始操作redis之前需要先锁住redis的情况
 * <ul>
 *  <li>需要重写exec方法，处理业务逻辑</li>
 * 	<li>需要重写getLockKey方法，返回锁的键值</li>
 *  <li>需要重写getLockTime方法，返回锁的时长，如果不重写，则默认为400秒</li>
 * </ul>
 * @author xiaobenhai
 */
public abstract class AbstractPrepareLockJob {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPrepareLockJob.class);
	
	@Resource
	private JedisLock jedisLock;
	
	public abstract void exec() throws Exception;
	
	public void execute(){
		if(!lockRedis(getLockKey(),getLockTime())){
			LOGGER.debug(" has running one [{}] task, check next time.", getLockKey());
			return;
		}
		try{
			exec();
			if(enableTimeLog()){
				LOGGER.info("JobTime:{},ClZ:{}", System.currentTimeMillis(),getClass().getSimpleName());
			}
		}catch (Exception e) {
			if(enableTimeLog()){
				LOGGER.info("PrepareLock error,JobTime:{},ClZ:{}", System.currentTimeMillis(),getClass().getSimpleName());
			}
			LOGGER.warn("refresh " +getLockKey() +" cache error:", e);
		}
	}
	
	/**
	 * @return 返回锁的键,默认返回类的全路径名
	 */
	protected String getLockKey(){
		return getClass().getName();
	}
	
	/**
	 * @return 返回锁的时长(秒)
	 */
	abstract protected int getLockTime();
	
	/**
	 * @return 是否打印耗时,默认true
	 */
	protected boolean enableTimeLog(){
		return true;
	}
	
	private boolean lockRedis(String key,int timeInSeconds){
		return jedisLock.lock(key, timeInSeconds);
	}
}
