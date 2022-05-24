package simpledb.tx.recovery;

import simpledb.buffer.Buffer;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static simpledb.tx.recovery.LogRecord.*;

/**
 * undo-only 恢复管理器，一个事务对应一个恢复管理器
 *
 *
 * @author shs
 * @date 2022/5/24 13:50
 */
public class RecoveryManager {

    private int txNum;

    /**
     * 为指定事务创建恢复管理器，写入事务开始日志
     *
     *
     * @param txNum
     * 事务号
     */
    public RecoveryManager(int txNum) {
        this.txNum = txNum;
        int lsn = new StartRecord(txNum).writeToLog();
        SimpleDB.logManager().flush(lsn);
    }

    /**
     * 提交事务
     */
    public void commit() {
        SimpleDB.bufferManager().flushAll(txNum);
        int lsn = new CommitRecord(txNum).writeToLog();
        SimpleDB.logManager().flush(lsn);
    }

    /**
     * 回滚事务
     */
    public void rollback() {
        SimpleDB.bufferManager().flushAll(txNum);
        doRollback();

        int lsn = new RollbackRecord(txNum).writeToLog();
        SimpleDB.logManager().flush(lsn);
    }

    /**
     * 数据库恢复
     */
    public void recovery() {
        SimpleDB.bufferManager().flushAll(txNum);
        doRecovery();

        int lsn = new CheckpointRecord().writeToLog();
        SimpleDB.logManager().flush(lsn);
    }

    /**
     * 写入SetInt日志记录
     *
     *
     * @param buffer
     * 事务写入int的缓冲区
     *
     * @param offset
     * 事务写入int的位置
     *
     * @param newVal
     * 事务写入int的值
     *
     * @return
     * SetInt日志记录的LSN
     */
    public int setIntRec(Buffer buffer, int offset, int newVal) {
        int oldVal = buffer.getInt(offset);
        Block blk = buffer.block();
        if (isTemporaryBlk(blk))
            return -1;
        return new SetIntRecord(txNum, offset, oldVal, blk).writeToLog();
    }

    /**
     * 写入SetString日志记录
     *
     *
     * @param buffer
     * 事务写入String的缓冲区
     *
     * @param offset
     * 事务写入String的位置
     *
     * @param newVal
     * 事务写入String的值
     *
     * @return
     * SetString日志记录的LSN
     */
    public int setStringRec(Buffer buffer, int offset, String newVal) {
        String oldVal = buffer.getString(offset);
        Block blk = buffer.block();
        if (isTemporaryBlk(blk))
            return -1;
        return new SetStringRecord(txNum, offset, oldVal, blk).writeToLog();
    }


    /**
     * 回滚当前事务
     * <p>
     * 从后至前，将当前事务的日志记录undo
     * </p>
     */
    void doRollback() {
        Iterator<LogRecord> it = new LogRecordIterator();
        while (it.hasNext()) {
            LogRecord rec = it.next();
            if (this.txNum == rec.txNumber()) {
                if (START == rec.operator())
                    return;
                rec.undo();
            }
        }
    }

    /**
     * 恢复数据库
     * <p>
     * 从后至前，将所有未commit/rollback的事务的日志记录undo
     * </p>
     */
    void doRecovery() {
        List<Integer> finishedTxs = new ArrayList<>();
        Iterator<LogRecord> it = new LogRecordIterator();
        while (it.hasNext()) {
            LogRecord rec = it.next();
            if (CHECKPOINT == rec.operator())
                return;
            if (finishedTxs.contains(rec.txNumber()))
                continue;
            if (COMMIT == rec.operator() || ROLLBACK == rec.operator())
                finishedTxs.add(rec.txNumber());
            else
                rec.undo();
        }
    }

    /**
     * 判断物理块是否属于临时文件
     *
     *
     * @param block
     * 指定物理块
     *
     * @return
     * 表示是否属于临时文件的布尔值
     */
    boolean isTemporaryBlk(Block block) {
        return block.getFilename().startsWith("temp");
    }
}
