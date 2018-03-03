package com.github.benhaixiao.commons.serializer.kryo;

/**
 * @author xiaobenhai
 */
public interface KryoPool {
	/**
	 * get o kryo object
	 *
	 * @return
	 */
	KryoHolder get();

	/**
	 * return object
	 *
	 * @param kryo
	 */
	void release(KryoHolder kryo);
	
	void releaseBroken(KryoHolder kryo);
	/**
	 * @return pool size
	 */
	
	long createCount();
	
	void close();
	
}
