package simpledb.log;

import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

import java.io.IOException;
import java.util.Iterator;

import static simpledb.file.Page.*;

/**
 * 日志管理类
 *
 *
 * @author shs
 * @date 2022/5/18 16:42
 */
public class LogManager {

    private static final int LAST_POS = 0;

    private Block currentBlk;
    private Page logPage = new Page();
    private String filename;
    private int currentPos;


    public LogManager(String logFilename) throws IOException{
        this.filename = logFilename;
        int logSize = SimpleDB.fileManager().size(logFilename);
        if (logSize == 0) {
            appendNewBlock();
        } else {
            currentBlk = new Block(filename, logSize - 1);
            logPage.read(currentBlk);
        }
        currentPos = getLastRecordPos() + INT_SIZE;
    }

    public int append(Object[] record) throws IOException{
        int recSize = INT_SIZE;
        for(Object obj : record) {
            recSize += size(obj);
        }
        if (currentPos + recSize > BLOCK_SIZE) {
            flush();
            appendNewBlock();
        } else {
            for (Object obj : record) {
                appendVal(obj);
            }
        }
        finalizeRecord();
        return currentLSN();
    }

    public void flush() {
        logPage.write(currentBlk);
    }
    
    public void flush(int lsn) {
        if (lsn >= currentLSN())
            flush();
    }

    private void appendNewBlock() throws IOException{
        Block newBlock = new Block(filename, 0);
        logPage.read(newBlock);
    }

    private int getLastRecordPos() {
        return logPage.getInt(LAST_POS);
    }

    private void setLastRecordPos(int pos) {
        logPage.setInt(LAST_POS, pos);
    }

    private int size(Object obj) {
        if (obj instanceof String) {
            String str = (String) obj;
            return STR_SIZE(str.length());
        }
        else if (obj instanceof Integer) {
            return INT_SIZE;
        } else {
            throw new RuntimeException("The log content has no match type: " + obj.getClass());
        }
    }

    private void appendVal(Object obj) {
        if (obj instanceof String) {
            logPage.setString(currentPos, (String) obj);
        } else if (obj instanceof Integer) {
            logPage.setInt(currentPos, (Integer) obj);
        }
        currentPos += size(obj);
    }

    private void finalizeRecord() {
        logPage.setInt(currentPos, getLastRecordPos());
        setLastRecordPos(currentPos);
        currentPos += INT_SIZE;
    }

    private int currentLSN() {
        return currentBlk.getBlkNum();
    }
}
