package com.ddu.demo.flink;

import com.ddu.demo.flink.avro.AvroSerializeUtil;
import com.ddu.demo.flink.avro.PojoBeanToAvroGenericRecordConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;

public class ParquetDemo {

    private static final String HDFS_PATH = ".";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String DATE_ZONE = "Asia/Shanghai";

    private static class BeanToGenericRecordMap extends RichMapFunction<String, GenericRecord> {

        transient PojoBeanToAvroGenericRecordConverter<MyBean> beanToRecordConverter;

        @Override
        public void open(Configuration parameters) throws Exception {
            beanToRecordConverter = new PojoBeanToAvroGenericRecordConverter<>(MyBean.class);;
        }

        @Override
        public GenericRecord map(String s) throws Exception {
            MyBean bean = new MyBean("abc");
            bean.setOptionalDate(new Date());
            return beanToRecordConverter.convert(bean);
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.enableCheckpointing(10000);

        Schema schema = AvroSerializeUtil.getSchema(MyBean.class);
        StreamingFileSink<GenericRecord> fileSink = StreamingFileSink
                .forBulkFormat(new Path(HDFS_PATH), ParquetAvroWriters.forGenericRecord(schema))
                .withBucketAssigner(new DateTimeBucketAssigner<>(DATE_FORMAT, ZoneId.of(DATE_ZONE)))
                .withBucketCheckInterval(100)
                .build();

        env.fromCollection(Arrays.asList("a", "aa", "aaa"))
                .map(new BeanToGenericRecordMap())
                .addSink(fileSink);

        env.execute("parquet-demo");
    }

}
