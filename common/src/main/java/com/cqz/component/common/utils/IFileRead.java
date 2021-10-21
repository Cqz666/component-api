package com.cqz.component.common.utils;

import java.io.IOException;

/**
 * IFileRead
 */
public interface IFileRead {
    void run(String content) throws IOException;

    void run(byte[] content) throws IOException;

    void tearDown() throws IOException;
}