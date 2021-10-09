package com.cqz.component.flink.sql.common;

import com.google.common.base.Strings;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.io.Charsets;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.List;

public class ExecuteProcessHelper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static List<URL> getExternalJarUrls(String addJarListStr) throws IOException {
        List<URL> jarUrlList = Lists.newArrayList();
        if (Strings.isNullOrEmpty(addJarListStr)) {
            return jarUrlList;
        }

        List<String> addJarFileList =
                OBJECT_MAPPER.readValue(
                        URLDecoder.decode(addJarListStr, Charsets.UTF_8.name()), List.class);
        // Get External jar to load
        for (String addJarPath : addJarFileList) {
            jarUrlList.add(new File(addJarPath).toURI().toURL());
        }
        return jarUrlList;
    }

    public static void main(String[] args) throws IOException, InvocationTargetException, IllegalAccessException {
        URLClassLoader urlClassLoader = (URLClassLoader) ExecuteProcessHelper.class.getClassLoader();
        List<URL> jarUrlList = ExecuteProcessHelper.getExternalJarUrls("[\"/home/chenqizhu/TestAPI-1.0-SNAPSHOT.jar\"]");
        ClassLoaderManager.loadExtraJar(jarUrlList, urlClassLoader);



    }

}
