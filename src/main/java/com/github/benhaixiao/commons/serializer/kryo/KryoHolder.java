package com.github.benhaixiao.commons.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.github.benhaixiao.commons.serializer.CRC16;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.DateSerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Kryo 的包装
 * @author xiaobenhai
 */
public class KryoHolder {
	private static final Logger logger = LoggerFactory.getLogger(KryoHolder.class);
	private static final int BUFFER_SIZE = 1024;

	Kryo kryo;
	Output output = new Output(BUFFER_SIZE, -1); // reuse
	Input input = new Input();

	private List<String> clazs;
	private String defaultSerializer = FieldSerializer.class.getName();

	public KryoHolder(List<String> clazs, String defaultSerializer) {
		this.clazs = clazs;
		if (StringUtils.isNotEmpty(defaultSerializer))
			this.defaultSerializer = defaultSerializer;
		init();
	}

	public void resetInput() {
		input.close();
		input = new Input();
	}

	public void resetOutput() {
		output.close();
		output = new Output(BUFFER_SIZE, -1);
	}

	private void init() {
		this.kryo = new Kryo();
		try {
			this.kryo.setDefaultSerializer((Class<? extends Serializer>) Class.forName(defaultSerializer));
		} catch (ClassNotFoundException e1) {
			logger.error("defaultSerializer not found:" + defaultSerializer, e1);
		}
		this.kryo.setRegistrationRequired(false);
		this.kryo.setReferences(false);// 小心,不能有循环的引用,不然会序列化失败!
		UnmodifiableCollectionsSerializer.registerSerializers(this.kryo);
		SynchronizedCollectionsSerializer.registerSerializers(this.kryo);
		this.kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
		this.kryo.register(Date.class, new DateSerializer(Date.class));
		this.kryo.register(HashMap.class, CRC16.calcCrc16("java.util.HashMap".getBytes()));
		this.kryo.register(Map.class, CRC16.calcCrc16("java.util.Map".getBytes()));
		this.kryo.register(LinkedHashMap.class, CRC16.calcCrc16("java.util.LinkedHashMap".getBytes()));
		this.kryo.register(List.class, CRC16.calcCrc16("java.util.List".getBytes()));
		this.kryo.register(ArrayList.class, CRC16.calcCrc16("java.util.ArrayList".getBytes()));

		if (clazs != null) {
			for (String claz : clazs) {
				try {
					int classId = CRC16.calcCrc16(claz.getBytes());
					if (this.kryo.getRegistration(classId) != null) {
						logger.error("注册kryo出错：存在相同classID的类，请赶紧处理！！{}", claz);
						continue;
					}
					this.kryo.register(Class.forName(claz), classId);
					logger.debug("register claz {} succ....=========================", claz);
				} catch (ClassNotFoundException e) {
					logger.error("", e);
				}
			}
		}
	}

}
