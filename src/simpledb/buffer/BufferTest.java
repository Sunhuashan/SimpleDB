package simpledb.buffer;

import simpledb.file.Block;
import simpledb.log.LogManager;
import simpledb.server.SimpleDB;

import java.io.IOException;

/**
 * @author shs
 * @date 2022/5/23 13:15
 */
public class BufferTest {
    public static void main(String[] args) throws IOException {
        SimpleDB.init("first_db");
        BufferManager bMg = SimpleDB.bufferManager();

        System.out.println("init, size of buffer pool is: " + bMg.available());
        Block block = new Block("stuTbl", 0);

        Buffer buffer = bMg.pin(block);
        System.out.println("after pin, size of buffer pool is: " + bMg.available());
        //读取缓冲区
        int intVal = buffer.getInt(0);
        String strVal = buffer.getString(16);
        System.out.println("int: " + intVal + "\nString: " + strVal);

        //修改缓冲区
        LogManager lMg = SimpleDB.logManager();
        //日志记录：filename | block number | offset | value before modify
        Object[] rec = {"stuTbl", 0, 0, intVal};
        int lsn = lMg.append(rec);

        //假想事务号
        int txNum = 10086;
        buffer.setInt(0,21, txNum, lsn);

        //持久化缓冲区和日志
        bMg.flushAll(txNum);

        bMg.unpin(buffer);
        System.out.println("after unpin, size of buffer pool is: " + bMg.available());

    }
}
