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
public abstract class PrefixKeyGenerator<T> extends FixKeyGenerator<T> {
	/**
	 * @param prefix
	 */
	public PrefixKeyGenerator(String prefix) {
		super(prefix);
	}
}
