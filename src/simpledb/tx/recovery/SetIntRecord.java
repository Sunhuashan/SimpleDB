package simpledb.tx.recovery;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferManager;
import simpledb.file.Block;
import simpledb.log.BasicLogRecord;
import simpledb.server.SimpleDB;

import java.util.Set;

/**
 * 写整数数据对应的格式化日志记录，日志格式为：
 * <p>
 * 【SET_INT（4），事务号，日志文件名，文件块号，块内偏移，整数数据】
 * </p>
 *
 * @author shs
 * @date 2022/5/23 21:16
 */
public class SetIntRecord implements LogRecord{

    private int txNum;
    private int offset;
    private int val;
    private Block blk;

    public SetIntRecord(int txNum, int offset, int val, Block blk) {
        this.txNum = txNum;
        this.offset = offset;
        this.val = val;
        this.blk = blk;
    }

    public SetIntRecord(BasicLogRecord blr) {
        //操作数已经在调用该方法前获取
        //int operator = blr.nextInt();
        this.txNum = blr.nextInt();
        this.blk = new Block(blr.nextString(), blr.nextInt());
        this.offset = blr.nextInt();
        this.val = blr.nextInt();
    }

    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{SET_INT, txNum, blk.getFilename(),
                blk.getBlkNum(), offset, val};
        return SimpleDB.logManager().append(rec);
    }

    @Override
    public int operator() {
        return SET_INT;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo() {
        BufferManager bMg = SimpleDB.bufferManager();
        Buffer buffer = bMg.pin(blk);
        buffer.setInt(offset, val, txNum, -1);
        bMg.unpin(buffer);
    }

    @Override
    public String toString() {
        return "<SET_INT" + txNum + " " + blk + " "
                + offset + " " + val + ">";
    }
}
