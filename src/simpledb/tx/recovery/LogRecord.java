package simpledb.tx.recovery;

import java.io.IOException;

/**
 * 日志记录接口
 *
 *
 * @author shs
 * @date 2022/5/23 19:05
 */
public interface LogRecord {
    /**
     * 不同的常量对应不同类型的日志记录
     */
    static final int CHECKPOINT = 0;
    static final int START = 1;
    static final int COMMIT = 2;
    static final int ROLLBACK = 3;
    static final int SET_INT = 4;
    static final int SET_STRING = 5;

    /**
     * 将日志记录写入日志文件中
     *
     *
     * @return
     * 日志记录对应的 LSN
     *
     * @throws IOException
     * I/O异常
     */
    public int writeToLog();

    /**
     * 获取当前日志记录的操作类型
     *
     *
     * @return
     * 代表日志记录类型的整型变量
     */
    public int operator();

    /**
     * 获取当前日志记录对应的事务号
     *
     *
     * @return
     * 事务号
     */
    public int txNumber();

    /**
     * 撤销该日志记录的操作
     * <p>
     * 仅对SET_INT,SET_STRING操作的日志记录生效
     * </p>
     */
    public void undo();
}
