package com.cqz.component.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLogPrint {
    private static final Logger LOG = LoggerFactory.getLogger(TestLogPrint.class);

    public static void main(String[] args) {

        LOG.error("log err");
        System.err.println("error print");

        LOG.info("log info");
        System.out.println("out print");

        //public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        //	return timestamp - (timestamp - offset + windowSize) % windowSize;
        //}

        System.out.println(1657711285000L -( (1657711285000L+(8*60*60*1000)+(7*24*60*60*1000))%(7*24*60*60*1000)));


    }
}
