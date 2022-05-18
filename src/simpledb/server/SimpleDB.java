package simpledb.server;

import simpledb.file.FileManager;

/**
 * @author shs
 * @date 2022/5/17 11:14
 */
public class SimpleDB {

    private static FileManager fmg;
    public static void init(String bdName) {
        fmg = new FileManager(bdName);
    }

    public static FileManager fileManager() {
        return fmg;
    }
}
