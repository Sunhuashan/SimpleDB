package simpledb.log;

import simpledb.file.FileManager;
import simpledb.server.SimpleDB;

import java.io.IOException;
import java.util.Iterator;

/**
 * 日志管理功能单元测试
 *
 *
 * @author shs
 * @date 2022/5/18 17:08
 */
public class LogTest {
    public static void main(String[] args) {
        String dbName = "first_db";
        SimpleDB.init(dbName);

        LogManager lMg = SimpleDB.logManager();

        Object[] log1 = {"a", "b"};
        Object[] log2 = {"c", "d"};


        lMg.append(log1);
        lMg.append(log2);


        lMg.flush();

        Iterator<BasicLogRecord> it = lMg.Iterator();
        while (it.hasNext()) {
            BasicLogRecord rec = it.next();
            System.out.println("log record: " + rec.nextString() + rec.nextString());
        }
    }
}
