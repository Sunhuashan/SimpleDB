package simpledb.tx.recovery;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferManager;
import simpledb.file.Block;
import simpledb.log.BasicLogRecord;
import simpledb.server.SimpleDB;

import java.io.IOException;

/**
 * 写字符串数据对应的格式化日志记录，日志格式为：
 * <p>
 * 【SET_STRING（5），事务号，日志文件名，文件块号，块内偏移，字符串数据】
 * </p>
 *
 *
 * @author shs
 * @date 2022/5/23 19:20
 */
public class SetStringRecord implements LogRecord{

    private int txNum;
    private int offset;
    private String val;
    private Block blk;

    /**
     * 构造函数
     *
     *
     * @param txNum
     * 事务号
     *
     * @param offset
     * 偏移量
     *
     * @param val
     * 字符串变量
     *
     * @param blk
     * 物理块引用{文件名，文件块号}
     */
    public SetStringRecord(int txNum, int offset, String val, Block blk) {
        this.txNum = txNum;
        this.offset = offset;
        this.val = val;
        this.blk = blk;
    }

    /**
     * 以BasicLogRecord对象为参数的构造函数
     *
     *
     * @param basicLogRec
     * 构造参数
     */
    public SetStringRecord(BasicLogRecord basicLogRec) {
        int operator = basicLogRec.nextInt();
        this.txNum = basicLogRec.nextInt();
        this.blk = new Block(basicLogRec.nextString(), basicLogRec.nextInt());
        this.offset = basicLogRec.nextInt();
        this.val = basicLogRec.nextString();
    }

    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{SET_STRING, txNum, blk.getFilename(), blk.getBlkNum(), offset, val};
        return SimpleDB.logManager().append(rec);
    }

    @Override
    public int operator() {
        return SET_STRING;
    }

    @Override
    public int txNumber() {
        return txNum;
    }

    @Override
    public void undo() {
        BufferManager bMg = SimpleDB.bufferManager();
        Buffer buffer = bMg.pin(blk);
        buffer.setString(offset, val, txNum, -1);
    }

    @Override
    public String toString() {
        return "<SET_STRING " + txNum + " " + blk + " "
                + offset + " " + val + ">";
    }
}
