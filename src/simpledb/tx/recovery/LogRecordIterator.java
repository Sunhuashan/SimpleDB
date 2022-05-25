package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;
import simpledb.server.SimpleDB;

import java.util.Iterator;

import static simpledb.tx.recovery.LogRecord.*;

/**
 * 格式化的日志记录迭代器
 *
 *
 * @author shs
 * @date 2022/5/24 9:54
 */
public class LogRecordIterator implements Iterator<LogRecord> {

    private Iterator<BasicLogRecord> it = SimpleDB.logManager().Iterator();

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public LogRecord next() {
        BasicLogRecord basicRec = it.next();
        int operator = basicRec.nextInt();
        switch (operator) {
            case CHECKPOINT:
                return new CheckpointRecord(basicRec);
            case START:
                return new StartRecord(basicRec);
            case COMMIT:
                return new CommitRecord(basicRec);
            case ROLLBACK:
                return new RollbackRecord(basicRec);
            case SET_INT:
                return new SetIntRecord(basicRec);
            case SET_STRING:
                return new SetStringRecord(basicRec);
            default:
                return null;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
