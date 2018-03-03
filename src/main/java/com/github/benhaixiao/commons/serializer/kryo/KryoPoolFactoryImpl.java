package com.github.benhaixiao.commons.serializer.kryo;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KryoPoolFactoryImpl implements KryoPool {

	private static final Logger logger = LoggerFactory.getLogger(KryoPoolFactoryImpl.class);

	private GenericObjectPool<KryoHolder> pool;
	private GenericObjectPoolConfig config;
	private boolean isInit = false;

	private List<String> clazs;
	private String defaultSerializer;

	public KryoPoolFactoryImpl() {
	}

	private synchronized void init() {
		if (!isInit) {
			if (config == null) {
				config = new GenericObjectPoolConfig();
				config.setMaxIdle(20);
				config.setMinIdle(20);
				config.setMaxTotal(500);
			}
			KryoObjectFactory factory = new KryoObjectFactory();
			pool = new GenericObjectPool<KryoHolder>(factory, config);
			isInit = true;
		}
	}

	public void setConfig(GenericObjectPoolConfig config) {
		this.config = config;
	}

	public void setRegisterClass(List<String> clazs) {
		this.clazs = clazs;
	}

	public void setDefaultSerializer(String defaultSerializer) {
		this.defaultSerializer = defaultSerializer;
	}

	public KryoHolder get() {
		try {
			if (!isInit)
				init();
			return (KryoHolder) pool.borrowObject();
		} catch (Exception e) {
			logger.error("fail to get KryoHolder", e);
			throw new RuntimeException(e);
		}
	}

	public void release(KryoHolder kryoHolder) {
		if (kryoHolder == null)
			return;
		try {
			pool.returnObject(kryoHolder);
		} catch (Exception e) {
			if (kryoHolder != null) {
				try {
					close(kryoHolder);
				} catch (Exception ex) {
					logger.warn(e.getMessage(), e);
				}
			}
		}
	}

	private void close(KryoHolder kryoHolder) {
		kryoHolder.kryo = null;
		if (kryoHolder.input != null)
			kryoHolder.input.close();
		if (kryoHolder.output != null)
			kryoHolder.output.close();
	}

	class KryoObjectFactory extends BasePooledObjectFactory<KryoHolder> {

		@Override
		public PooledObject<KryoHolder> wrap(KryoHolder obj) {
			return new DefaultPooledObject<KryoHolder>(obj);
		}

		@Override
		public KryoHolder create() throws Exception {
			return new KryoHolder(clazs, defaultSerializer);
		}

		@Override
		public void destroyObject(PooledObject<KryoHolder> p) throws Exception {
			close(p.getObject());
		}
	}

	@Override
	public long createCount() {
		return pool.getCreatedCount();
	}

	@Override
	public void releaseBroken(KryoHolder kryo) {
		try {
			pool.invalidateObject(kryo);
		} catch (Exception e) {
			logger.warn("fail to invalid object:{}", kryo, e);
		}
	}

	@Override
	public void close() {
		pool.close();
	}

}
