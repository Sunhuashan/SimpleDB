package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;

/**
 * @author shs
 * @date 2022/5/24 10:01
 */
public class StartRecord implements LogRecord{

    public StartRecord(BasicLogRecord blr) {

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
