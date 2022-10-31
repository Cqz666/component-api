package com.cqz.component.flink.connector.datagen;

import net.sf.cglib.beans.BeanGenerator;

import java.util.Map;

public class CGLibUtil {

    public static Class< ? > createBeanClass(final String className, final Map<String, Class< ? >> properties) {

        final BeanGenerator beanGenerator = new BeanGenerator();

        /* use our own hard coded class name instead of a real naming policy */
        beanGenerator.setNamingPolicy((prefix, source, key, names) -> className);

        BeanGenerator.addProperties(beanGenerator, properties);
        return (Class< ? >) beanGenerator.createClass();
    }

}
