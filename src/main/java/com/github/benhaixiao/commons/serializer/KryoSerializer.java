package com.github.benhaixiao.commons.serializer;

import com.github.benhaixiao.commons.serializer.kryo.KryoUtils;

import java.io.Serializable;
/**
 * @author xiaobenhai
 */
public class KryoSerializer implements Serializater{
	
	private KryoUtils kryoUtils;
	
	@Override
	public byte[] serialize(Serializable obj) {
		return kryoUtils.kryoSerialize(obj);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T deserialize(byte[] objectData) {
		return (T) kryoUtils.kryoDeserialize(objectData);
	}

	public void setKryoUtils(KryoUtils kryoUtils) {
		this.kryoUtils = kryoUtils;
	}

}
