package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;
import simpledb.server.SimpleDB;

/**
 * 事务提交对应的格式化日志记录，记录格式为：
 * <p>
 * 【COMMIT（2），事务号】
 * </p>
 *
 *
 * @author shs
 * @date 2022/5/24 10:02
 */
public class CommitRecord implements LogRecord{

    private int txNum;

    public CommitRecord(int txNum) {
        this.txNum = txNum;
    }

    public CommitRecord(BasicLogRecord blr) {
        //操作数已经在调用该方法前获取
        //int operator = blr.nextInt();
        this.txNum = blr.nextInt();
    }

    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{COMMIT, txNum};
        return SimpleDB.logManager().append(rec);
    }

    @Override
    public int operator() {
        return COMMIT;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo() {

    }
}
