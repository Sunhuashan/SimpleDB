package simpledb.tx.recovery;

/**
 * @Author: shs
 * @Data: 2022/5/23 19:05
 */
public interface LogRecord {
    static final int CHECKPOINT = 0;
    static final int START = 1;
    static final int COMMIT = 2;
    static final int ROLLBACK = 3;
    static final int SET_INT = 4;
    static final int SET_STRING = 5;

    public void writeToLog();
    public int operation();
    public int txNumber();
    public void undo();
}
