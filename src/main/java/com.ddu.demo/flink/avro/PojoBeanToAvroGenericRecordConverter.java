package com.ddu.demo.flink.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author wxl24life
 */
public class PojoBeanToAvroGenericRecordConverter<E> {
    private final PropertyExtractors extractors = new PropertyExtractors();

    private final Class<?> type;
    private final Schema schema;

    public PojoBeanToAvroGenericRecordConverter(Class<E> type) {
        this.type = type;
        this.schema = ReflectData.get().getSchema(type);
    }

    public PojoBeanToAvroGenericRecordConverter(Class<E> type, Schema schema) {
        this.type = type;
        this.schema = schema;
    }

    public GenericRecord convert(E bean) {
        try {
            return convertBeanToRecord(bean, schema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private GenericRecord convertBeanToRecord(Object bean, Schema schema) throws Exception {
        GenericRecord result = new GenericData.Record(schema);
        PropertyExtractors.PropertyExtractor extractor = extractors.getOrCreate(bean.getClass());
        for (Schema.Field field : schema.getFields()) {
            Schema fieldSchema = field.schema();
            Schema.Type type = fieldSchema.getType();
            String name = field.name();

            Class<?> propertyTypeClazz = extractor.extractPropertyType(bean, name);
            Object value = extractor.extract(bean, name);

            // 对 java.util.Date 类型的属性做特殊的处理，转换成 Long 类型（millseconds)
            // hive 做查询时，可以使用 from_unixtime 对数据做一次转换成日期串
            if (propertyTypeClazz == Date.class && value == null) {
                value = 0L;
            } else if (propertyTypeClazz == Date.class) {
                value = ((Date) value).getTime(); // millseconds since 1970-01-01
            }

            if (type.equals(Schema.Type.UNION)) {
                List<Schema> types = fieldSchema.getTypes();
                int index = 0;
                for (Schema subType : types) {
                    if (subType.getType().equals(Schema.Type.NULL)) {
                        index++;
                        continue;
                    }
                    type = subType.getType();
                    fieldSchema = fieldSchema.getTypes().get(index);
                    break;
                }
            }

            if (isSimpleType(type)) {
                result.put(name, value);
                continue;
            }

            if (type.equals(Schema.Type.RECORD)) {
                GenericRecord fieldRes = convertBeanToRecord(value, fieldSchema);
                result.put(name, fieldRes);
                continue;
            }

            if (type.equals(Schema.Type.ARRAY)) {
                // let's assume it's always list
                List<Object> elements = (List<Object>) value;
                Schema elementSchema = fieldSchema.getElementType();

                if (isSimpleType(elementSchema.getType())) {
                    result.put(name, elements);
                    continue;
                }

                List<GenericRecord> results = new ArrayList<>(elements.size());

                for (Object element : elements) {
                    GenericRecord elementRes = convertBeanToRecord(element, elementSchema);
                    results.add(elementRes);
                }

                result.put(name, results);
            }
        }

        return result;
    }

    private static boolean isSimpleType(Schema.Type type) {
        if (type.equals(Schema.Type.STRING)) {
            return true;
        }
        if (type.equals(Schema.Type.INT)) {
            return true;
        }
        return type.equals(Schema.Type.LONG);

    }

}