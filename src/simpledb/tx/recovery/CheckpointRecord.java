package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;
import simpledb.server.SimpleDB;

/**
 * 检查点类型日志记录，格式为:【CHECKPOINT（0）】
 * <p>
 * 在检查点日志记录之前的日志是无需恢复的。每次数据库恢复完成后，
 * 添加检查点日志记录，以此减少恢复过程的工作量
 * </p>
 *
 *
 * @author shs
 * @date 2022/5/24 10:00
 */
public class CheckpointRecord implements LogRecord{


    public CheckpointRecord() {

    }

    public CheckpointRecord(BasicLogRecord blr) {

    }

    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{CHECKPOINT};
        return SimpleDB.logManager().append(rec);
    }

    @Override
    public int operator() {
        return CHECKPOINT;
    }

    @Override
    public int txNumber() {
        return -1;
    }

    @Override
    public void undo() {

    }
}
