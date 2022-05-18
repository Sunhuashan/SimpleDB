package simpledb;

import simpledb.file.FileManager;

/**
 * @author shs
 * @date 2022/5/17 11:14
 */
public class SimpleDB {


    public static FileManager fileManager() {
        return new FileManager("first_db");
    }
}
