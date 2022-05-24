package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;

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

    }

    @Override
    public int writeToLog() {
        return 0;
    }

    @Override
    public int operator() {
        return 0;
    }

    @Override
    public int txNumber() {
        return 0;
    }

    @Override
    public void undo() {

    }
}
