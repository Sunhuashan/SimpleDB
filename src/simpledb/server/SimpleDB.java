package simpledb.server;

import simpledb.buffer.BasicBufferManager;
import simpledb.buffer.BufferManager;
import simpledb.file.FileManager;
import simpledb.log.LogManager;

import java.io.IOException;

/**
 * @author shs
 * @date 2022/5/17 11:14
 */
public class SimpleDB {
    private static final int BUFFER_POOL_SIZE = 10;

    private static FileManager fMg;
    private static LogManager lMg;
    private static BufferManager bMg;

    public static void init(String bdName) {
        fMg = new FileManager(bdName);

        String logFilename = bdName + "_log";
        try {
            lMg = new LogManager(logFilename);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot create log manager named " + logFilename);
        }

        BasicBufferManager bbm = new BasicBufferManager(BUFFER_POOL_SIZE);
        bMg = new BufferManager(bbm);
    }

    public static FileManager fileManager() {
        return fMg;
    }
    public static LogManager logManager() {
        return lMg;
    }
    public static BufferManager bufferManager() {
        return bMg;
    }
}
