package com.github.benhaixiao.commons.serializer;

import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
/**
 * @author xiaobenhai
 */
public class JavaSerializer implements Serializater{

	@Override
	public byte[] serialize(Serializable obj) {
		return SerializationUtils.serialize(obj);
	}

	@Override
	public <T> T deserialize(byte[] objectData) {
		return SerializationUtils.deserialize(objectData);
	}

}
