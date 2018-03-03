package com.github.benhaixiao.commons.serializer.fst;

import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author xiaobenhai
 */
public class FstUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(FstUtils.class);

	
	/**
	 * <p>
	 * Serializes an <code>Object</code> to a byte array for storage/serialization.
	 * </p>
	 *
	 * @param obj
	 *            the object to serialize to bytes
	 * @return a byte[] with the converted Serializable
	 */
	public static byte[] fastSerialize(Object obj) {
		ByteArrayOutputStream byteArrayOutputStream = null;
		FSTObjectOutput out = null;
		try {
			// stream closed in the finally
			byteArrayOutputStream = new ByteArrayOutputStream(512);
			out = new FSTObjectOutput(byteArrayOutputStream); // 32000 buffer size
			out.writeObject(obj);
			out.flush();
			return byteArrayOutputStream.toByteArray();
		} catch (IOException ex) {
			LOGGER.error("",ex);
		} finally {
			try {
				obj = null;
				if (out != null) {
					out.close(); // call flush byte buffer
					out = null;
				}
				if (byteArrayOutputStream != null) {

					byteArrayOutputStream.close();
					byteArrayOutputStream = null;
				}
			} catch (IOException ex) {
				// ignore close exception
			}
		}
		return null;
	}

	// Deserialize
	// -----------------------------------------------------------------------

	/**
	 * <p>
	 * Deserializes a single <code>Object</code> from an array of bytes.
	 * </p>
	 *
	 * @param objectData
	 *            the serialized object, must not be null
	 * @return the deserialized object
	 * @throws IllegalArgumentException
	 *             if <code>objectData</code> is <code>null</code>
	 */
	public static Object fastDeserialize(byte[] objectData){
		ByteArrayInputStream byteArrayInputStream = null;
		FSTObjectInput in = null;
		try {
			// stream closed in the finally
			byteArrayInputStream = new ByteArrayInputStream(objectData);
			in = new FSTObjectInput(byteArrayInputStream);
			return in.readObject();
		} catch (ClassNotFoundException ex) {
			LOGGER.error("",ex);
		} catch (IOException ex) {
			LOGGER.error("",ex);
		} finally {
			try {
				objectData = null;
				if (in != null) {
					in.close();
					in = null;
				}
				if (byteArrayInputStream != null) {
					byteArrayInputStream.close();
					byteArrayInputStream = null;
				}
			} catch (IOException ex) {
				// ignore close exception
			}
		}
		return null;
	}

}
