package com.ddu.demo.flink.avro.test;

import com.ddu.demo.flink.MyBean;
import com.ddu.demo.flink.avro.AvroSerializeUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author wxl24life
 */
public class AvroSerializeUtilTest {

    @Test
    public void testActivityInfoBeanSerializeAndDesiralize() throws IOException {
        MyBean myBean = new MyBean();
        myBean.setValue("aaa");
        myBean.setOptionalDate(new Date());

        byte[] bytes = AvroSerializeUtil.serialize(myBean, MyBean.class);
        System.out.println(bytes.length);

        MyBean deserializeObj = AvroSerializeUtil.deserialize(bytes, MyBean.class);

        assertNotNull(deserializeObj.getOptionalDate());
        assertEquals(myBean.getOptionalDate(), deserializeObj.getOptionalDate());
        assertEquals(myBean.getValue(), deserializeObj.getValue());
    }

}
