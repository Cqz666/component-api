package com.cqz.component.flink.sql.common;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

public class ClassLoaderManager {

    public static URLClassLoader loadExtraJar(List<URL> jarUrlList, URLClassLoader classLoader)
            throws IllegalAccessException, InvocationTargetException {
        for (URL url : jarUrlList) {
            if (url.toString().endsWith(".jar")) {
                urlClassLoaderAddUrl(classLoader, url);
            }
        }
        return classLoader;
    }

    private static void urlClassLoaderAddUrl(URLClassLoader classLoader, URL url)
            throws InvocationTargetException, IllegalAccessException {
        Method method = ReflectionUtils.getDeclaredMethod(classLoader, "addURL", URL.class);

        if (method == null) {
            throw new RuntimeException(
                    "can't not find declared method addURL, curr classLoader is "
                            + classLoader.getClass());
        }
        method.setAccessible(true);
        method.invoke(classLoader, url);
    }

}
