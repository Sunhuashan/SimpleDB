package simpledb.log;

import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

import java.io.IOException;
import java.util.Iterator;

import static simpledb.file.Page.*;

/**
 * 负责将日志记录写入到日志文件中
 *
 *
 * @author shs
 * @date 2022/5/18 16:42
 */
public class LogManager {

    // [LAST_POS, LAST_POS + 3]四个字节组成一个整数
    // 记录了当前日志缓冲区有效数据最后位置
    static final int LAST_POS = 0;
    // 日志文件读入内存中的物理块
    private Block currentBlk;
    // 日志文件的内存缓冲区
    private Page logPage = new Page();
    // 日志文件名
    private String filename;
    // 指向当前日志缓冲区最后写入位置的指针
    private int currentPos;


    /**
     * 通过文件名构造日志管理类
     *
     *
     * @param logFilename
     * 日志文件名
     *
     * @throws IOException
     * 读写异常
     */
    public LogManager(String logFilename) throws IOException{
        this.filename = logFilename;
        int logSize = SimpleDB.fileManager().size(logFilename);
        if (logSize == 0) {
            appendNewBlock();
        } else {
            currentBlk = new Block(filename, logSize - 1);
            logPage.read(currentBlk);
        }
        currentPos = getLastRecordPos() + INT_SIZE;
    }

    /**
     * 在日志缓冲区追加一条日志记录
     *
     *
     * @param record
     * 待追加的日志记录
     *
     * @return
     * 写入的记录的 LSN(Log Sequence Number)
     * 标识一条日志记录
     *
     * @throws IOException
     * 读写异常
     */
    public int append(Object[] record) throws IOException{
        int recSize = INT_SIZE;
        for(Object obj : record) {
            recSize += size(obj);
        }
        if (currentPos + recSize > BLOCK_SIZE) {
            flush();
            appendNewBlock();
        } else {
            for (Object obj : record) {
                appendVal(obj);
            }
        }
        finalizeRecord();
        return currentLSN();
    }

    /**
     * 将当前缓冲区写入当前物理块中
     */
    public void flush() {
        logPage.write(currentBlk);
    }

    /**
     * 将指定的日志记录写入物理块中
     *
     *
     * @param lsn
     * 标识日志记录
     */
    public void flush(int lsn) {
        if (lsn >= currentLSN())
            flush();
    }

    /**
     * 在日志文件中追加一个物理块
     *
     *
     * @throws IOException
     * 读写异常
     */
    private void appendNewBlock() throws IOException{
        Block newBlock = new Block(filename, 0);
        logPage.read(newBlock);
    }

    /**
     * 获取当前日志缓冲区有效数据最后位置
     *
     *
     * @return
     * 末尾指针位置
     */
    private int getLastRecordPos() {
        return logPage.getInt(LAST_POS);
    }

    /**
     * 设置当前日志缓冲区有效数据最后位置
     *
     *
     * @param pos
     * 末尾指针位置
     */
    private void setLastRecordPos(int pos) {
        logPage.setInt(LAST_POS, pos);
    }

    /**
     * 获取Object对象的字节数，用来计算一条日志记录(Object[] 对象)的字节总数
     *
     *
     * @param obj
     * Object对象，支持：int、 String
     *
     * @return
     * Object对象的字节数
     */
    private int size(Object obj) {
        if (obj instanceof String) {
            String str = (String) obj;
            return STR_SIZE(str.length());
        }
        else if (obj instanceof Integer) {
            return INT_SIZE;
        } else {
            throw new RuntimeException("The log content has no match type: " + obj.getClass());
        }
    }

    /**
     * 向内存缓冲区追加一个Object对象，用来追加一条日志记录
     *
     *
     * @param obj
     * Object对象，支持：int、 String
     */
    private void appendVal(Object obj) {
        if (obj instanceof String) {
            logPage.setString(currentPos, (String) obj);
        } else if (obj instanceof Integer) {
            logPage.setInt(currentPos, (Integer) obj);
        }
        currentPos += size(obj);
    }

    /**
     * 每追加一条日志记录后的操作
     * <p>
     * 向内存缓冲区追加一条日志记录后，在该记录后四个字节写入一个整数
     * 该整数为插入该记录前的整个缓冲区末尾位置，即上一条记录末尾的后一位的位置，
     * 该位置往后记录一个整数，这个整数是上一条记录的起始位置。
     * 之后，修改缓冲区末尾位置和当前位置的值
     * </p>
     */
    private void finalizeRecord() {
        logPage.setInt(currentPos, getLastRecordPos());
        setLastRecordPos(currentPos);
        currentPos += INT_SIZE;
    }

    /**
     * 获取当前记录的 LSN
     * <p>
     * 为了简化操作，这里将LSN的值设置为当前块号
     * </p>
     *
     *
     * @return
     * LSN 日志序号
     */
    private int currentLSN() {
        return currentBlk.getBlkNum();
    }
}
