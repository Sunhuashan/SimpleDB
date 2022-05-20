package simpledb.server;

import simpledb.file.FileManager;
import simpledb.log.LogManager;

import java.io.IOException;

/**
 * @author shs
 * @date 2022/5/17 11:14
 */
public class SimpleDB {

    private static FileManager fMg;
    private static LogManager lMg;

    public static void init(String bdName) {
        fMg = new FileManager(bdName);

        String logFilename = bdName + "_log";
        try {
            lMg = new LogManager(logFilename);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot create log manager named " + logFilename);
        }
    }

    public static FileManager fileManager() {
        return fMg;
    }
    public static LogManager logManager() {
        return lMg;
    }
}
