package com.ddu.demo.flink;

import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.DateAsLongEncoding;
import org.apache.avro.reflect.Nullable;

import java.util.Date;

/**
 * @author wxl24life
 */
public class MyBean {
    @Nullable
    private String value;

    // @Nullable not work here
    @AvroEncode(using=DateAsLongEncoding.class)
    private Date optionalDate;

    public MyBean() {}

    public MyBean(String value) {
        this.value = value;
    }

    public void setOptionalDate(Date optionalDate) {
        this.optionalDate = optionalDate;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Date getOptionalDate() {
        return optionalDate;
    }

    @Override
    public String toString() {
        return value + ", optionalDate = " + optionalDate;
    }
}
