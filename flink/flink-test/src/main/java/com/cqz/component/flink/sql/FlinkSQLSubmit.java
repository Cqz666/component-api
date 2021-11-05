package com.cqz.component.flink.sql;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


public class FlinkSQLSubmit {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQLSubmit.class);

    public static final String PROPERTIES_FILE_NAME = "application.properties";

    public static void main(String[] args) throws Exception {
        if (args.length<1){
            System.out.println("there is not enough args,Usage sql,");
            return;
        }
        LOG.info("------------program params-------------------------");
        Arrays.stream(args).forEach(arg -> LOG.info("args: {}", arg));
        LOG.info("-------------------------------------------");
        //整合参数
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(FlinkSQLSubmit.class.getResourceAsStream(PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties());


    }
}
