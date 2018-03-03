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
package com.github.benhaixiao.commons.redis;

/**
 * @author xiaobenhai
 */
public abstract class FixKeyGenerator<T> implements KeyGenerator<T> {
	protected String fix;
	public FixKeyGenerator(String fix){
		this.fix = fix;
	}
	
	public String getFix() {
		return fix;
	}
	public void setFix(String fix) {
		this.fix = fix;
	}
}
