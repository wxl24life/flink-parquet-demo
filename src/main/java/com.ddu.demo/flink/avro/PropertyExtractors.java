package com.ddu.demo.flink.avro;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wxl24life
 */
class PropertyExtractors {

    static class PropertyExtractor {
        private final Class<?> type;
        private final Map<String, Method> getters;
        private final Map<String, Class<?>> propertyTypes;

        private PropertyExtractor(Class<?> type, Map<String, Method> getters, Map<String, Class<?>> propertyTypes) {
            this.type = type;
            this.getters = getters;
            this.propertyTypes = propertyTypes;
        }

        Object extract(Object bean, String propertyName) {
            try {
                return getters.get(propertyName).invoke(bean);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        Class<?> extractPropertyType(Object bean, String propertyName) {
            try {
                return propertyTypes.get(propertyName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public Class<?> getType() {
            return type;
        }
    }

    private final Map<Class<?>, PropertyExtractor> cache = new HashMap<>();

    PropertyExtractor getOrCreate(Class<?> type) {
        if (cache.containsKey(type)) {
            return cache.get(type);
        }

        PropertyExtractor extractor = forClass(type);
        cache.put(type, extractor);
        return extractor;
    }

    private static PropertyExtractor forClass(Class<?> type) {
        try {
            return forClassNotSafe(type);
        } catch (IntrospectionException e) {
            throw new RuntimeException(e);
        }
    }

    private static PropertyExtractor forClassNotSafe(Class<?> type) throws IntrospectionException {
        BeanInfo info = Introspector.getBeanInfo(type);
        PropertyDescriptor[] properties = info.getPropertyDescriptors();

        Map<String, Method> getters = new HashMap<>();
        Map<String, Class<?>> propertyTypes = new HashMap<>();

        for (PropertyDescriptor pd : properties) {
            String name = pd.getName();
            if ("class".equals(name)) {
                continue;
            }

            Method getter = pd.getReadMethod();
            if (getter == null) {
                continue;
            }

            Class<?> returnType = getter.getReturnType();
            propertyTypes.put(name, returnType);

            getters.put(name, getter);
        }

        return new PropertyExtractor(type, getters, propertyTypes);
    }
}
