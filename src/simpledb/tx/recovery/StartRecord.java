package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;
import simpledb.server.SimpleDB;

/**
 * 事务开始日志记录，格式为：【START（1），事务号】
 *
 *
 * @author shs
 * @date 2022/5/24 10:01
 */
public class StartRecord implements LogRecord{

    private int txNum;

    public StartRecord(int txNum) {
        this.txNum = txNum;
    }

    public StartRecord(BasicLogRecord blr) {
        //操作数已经在调用该方法前获取
        //int operator = blr.nextInt();
        this.txNum = blr.nextInt();
    }

    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{START, txNum};
        return SimpleDB.logManager().append(rec);
    }

    @Override
    public int operator() {
        return START;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo() {

    }
}
