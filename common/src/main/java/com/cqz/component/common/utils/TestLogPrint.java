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

    }
}
