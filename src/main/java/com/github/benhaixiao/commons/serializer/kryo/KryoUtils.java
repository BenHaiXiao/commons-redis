package com.github.benhaixiao.commons.serializer.kryo;

/**
 * kryo序列化工具
 *
 * @author xiaobenhai
 * 
 */
public class KryoUtils {

	private KryoPool kryoPool;

	public void setKryoPool(KryoPool kryoPool) {
		this.kryoPool = kryoPool;
	}

	/**
	 * 获取对象创建次数
	 * 
	 * 
	 * @author zhengdongjian@yy.com 2016年1月28日
	 * @return
	 */
	public long getCreateCount() {
		return kryoPool.createCount();
	}

	/**
	 * 将对象序列化为字节数组
	 *
	 * @param obj
	 * @return 字节数组
	 */
	public byte[] kryoSerialize(Object obj) {
		KryoHolder kryoHolder = null;
		if (obj == null)
			return null;
		boolean success = false;
		try {
			kryoHolder = kryoPool.get();
			kryoHolder.kryo.writeClassAndObject(kryoHolder.output, obj);
			byte[] rt = kryoHolder.output.toBytes();
			success = true;
			return rt;
		} finally {
			if (kryoHolder != null) {
				if (success) {
					kryoHolder.resetOutput();
					kryoPool.release(kryoHolder);
				} else {
					kryoPool.releaseBroken(kryoHolder);
				}
			}
		}
	}

	/**
	 * 将字节数组反序列化为对象
	 *
	 * @param bytes
	 *            字节数组
	 * @return object
	 */
	public Object kryoDeserialize(byte[] bytes) {
		KryoHolder kryoHolder = null;
		if (bytes == null)
			return null;
		boolean success = false;
		try {
			kryoHolder = kryoPool.get();
			kryoHolder.input.setBuffer(bytes, 0, bytes.length);
			Object rt = kryoHolder.kryo.readClassAndObject(kryoHolder.input);
			success = true;
			return rt;
		} finally {
			if (kryoHolder != null) {
				if (success) {
					kryoHolder.resetInput();
					kryoPool.release(kryoHolder);
				} else {
					kryoPool.releaseBroken(kryoHolder);
				}
			}
		}
	}

}
