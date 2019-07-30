package com.ddu.demo.flink.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * 采用 avro 二进制编码协议对 Java POJO 对象进行序列化和反序列化<br/>
 * NOTE: POJO类里需要有无参构造器<p/>
 *
 * 举例：<br/>
 * <code>
 *     byte[] bytes = AvroSerializeUtil.serialize(activityInfoBean, ActivityInfoBean.class);
 *     System.out.println(bytes.length);
 *     ActivityInfoBean deserializeObj = AvroSerializeUtil.deserialize(bytes, ActivityInfoBean.class);
 *     System.out.println(deserializeObj);
 * </code>
 * @author wxl24life
 */
public class AvroSerializeUtil {

    /**
     * 将 Java POJO 对象以avro序列化的方式转换成字节数组
     * @param object 待序列化的 Java POJO 类实例对象
     * @param cls Java POJO 类
     * @param <T> POJO类的类型名称
     * @return 序列化后的字节数组
     */
    public static <T> byte[] serialize(final T object, final Class<T> cls) {
        final Schema schema = getSchema(cls);
        return serialize(object, schema, cls);
    }

    /**
     * 将 Java POJO 对象以avro序列化的方式转换成字节数组
     * @param object 待序列化的 Java POJO 类实例对象
     * @param cls Java POJO 类
     * @param <T> POJO类的类型名称
     * @return 序列化后的字节数组
     */
    public static <T> byte[] serialize(final T object, final Schema schema, final Class<T> cls) {
        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        final DatumWriter<T> writer = new ReflectDatumWriter<T>(schema);
        final BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);
        try {
            writer.write(object, binEncoder);
            binEncoder.flush();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return bout.toByteArray();
    }


    /**
     * 使用 avro 反射的方式进行反序列化
     * @param bytes 序列化后的字节数组
     * @param cls Java POJO 类
     * @param <T> POJO类的类型名称
     * @return 反序列化出来的实体对象
     */
    public static <T> T deserialize(byte[] bytes, final Class<T> cls) {
        final Schema schema = getSchema(cls);
        return deserialize(bytes, schema, cls);
    }

    /**
     * 使用 avro 反射的方式进行反序列化
     * @param bytes 序列化后的字节数组
     * @param <T> POJO类的类型名称
     * @return 反序列化出来的实体对象
     */
    public static <T> T deserialize(byte[] bytes, final Schema schema, final Class<T> cls) {
        DatumReader<T> reader = new ReflectDatumReader<T>(schema);
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        try {
            T object = reader.read(null, decoder);
            return object;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 使用 avro 反射的方式进行反序列化
     * @param bytes 序列化后的字节数组
     * @param writerSchema writer schema
     * @param readerSchema
     * @return 反序列化出来的实体对象
     */
    public static Object deserialize(byte[] bytes, final Schema writerSchema, final Schema readerSchema) {
        ReflectDatumReader reflectDatumReader = new ReflectDatumReader(writerSchema, readerSchema);
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);

        try {
            Object read = reflectDatumReader.read(null, decoder);
            return read;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 使用Reflect API获取java class关联的Schema
     * @param cls
     * @return
     */
    public static Schema getSchema(final Class cls) {
        return  ReflectData.get().getSchema(cls);
    }
}
