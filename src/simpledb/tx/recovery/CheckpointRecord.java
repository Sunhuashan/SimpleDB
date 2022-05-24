package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;

/**
 * @author shs
 * @date 2022/5/24 10:00
 */
public class CheckpointRecord implements LogRecord{

    public CheckpointRecord(BasicLogRecord blr) {

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
