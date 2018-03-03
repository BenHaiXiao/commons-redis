package com.github.benhaixiao.commons.serializer;

import com.github.benhaixiao.commons.serializer.fst.FstUtils;

import java.io.Serializable;
/**
 * @author xiaobenhai
 */
public class FstSerializer implements Serializater{

	@Override
	public byte[] serialize(Serializable obj) {
		return FstUtils.fastSerialize(obj);
	}

	@SuppressWarnings("unchecked")
    @Override
	public <T> T deserialize(byte[] objectData) {
		return (T) FstUtils.fastSerialize(objectData);
	}

	public static void main(String[] args) {
        FstSerializer f = new FstSerializer();
        Object o = f;
        System.out.println(o.getClass());
    }
}
