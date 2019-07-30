package com.ddu.demo.flink.avro.test;

import com.ddu.demo.flink.MyBean;
import com.ddu.demo.flink.avro.PojoBeanToAvroGenericRecordConverter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author wxl24life
 */
public class PojoBeanToAvroGenericRecordConverterTest {

    @Test
    public void testConvert() {
        MyBean myBean = new MyBean("aaa");
        PojoBeanToAvroGenericRecordConverter<MyBean> beanToRecordConverter = new PojoBeanToAvroGenericRecordConverter<>(MyBean.class);
        GenericRecord genericRecord = beanToRecordConverter.convert(myBean);

        assertNotNull(genericRecord);
        assertEquals("aaa", genericRecord.get("value"));
        assertEquals(0L, genericRecord.get("optionalDate"));
    }
}
