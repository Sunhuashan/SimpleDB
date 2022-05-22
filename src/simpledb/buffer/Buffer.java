package simpledb.buffer;

import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.log.LogManager;
import simpledb.server.SimpleDB;



/**
 * 缓冲区对象，维护内容和此缓冲区的状态信息
 *
 *
 * @author shs
 * @date 2022/5/22 14:13
 */
public class Buffer {

    //缓冲区内的数据
    private Page contents = new Page();
    //调入该缓冲区的物理块
    private Block myBlk = null;
    //正使用该缓冲区的事务个数
    private int pins = 0;
    //修改该缓冲区的事务编号
    private int modifiedBy = -1;
    //缓冲区对应的日志序号
    private int logSequenceNum = -1;


    /**
     * 获取该调入该缓冲区的物理块
     *
     *
     * @return
     * 物理块对象
     */
    public Block block() {
        return myBlk;
    }

    public String getString(int offset) {
        return contents.getString(offset);
    }

    public int getInt(int offset) {
        return contents.getInt(offset);
    }

    /**
     * 向缓冲区写入字符串
     *
     *
     * @param offset
     * 数据写入位置
     *
     * @param val
     * 字符串变量
     *
     * @param txNum
     * 修改缓冲区的事务号
     *
     * @param LSN
     * 此次写入对应的日志记录序号
     */
    public void setString(int offset, String val, int txNum, int LSN) {
        modifiedBy = txNum;
        if (LSN >= 0)
            logSequenceNum = LSN;
        contents.setString(offset, val);
    }

    /**
     * 向缓冲区写入整数
     *
     *
     * @param offset
     * 数据写入位置
     *
     * @param val
     * 整数变量
     *
     * @param txNum
     * 修改缓冲区的事务号
     *
     * @param LSN
     * 此次写入对应的日志记录序号
     */
    public void setInt(int offset, int val, int txNum, int LSN) {
        modifiedBy = txNum;
        if (LSN >= 0)
            logSequenceNum = LSN;
        contents.setInt(offset, val);
    }

    /**
     * 判断此缓冲区是否被指定事务所修改
     *
     *
     * @param txNum
     * 指定事务号
     *
     * @return
     * 标识是否被指定事务修改的布尔值
     */
    boolean isModifiedBy(int txNum) {
        return modifiedBy == txNum;
    }

    /**
     * 事务标识对此缓冲区的访问
     */
    void pin() {
        pins++;
    }

    /**
     * 事务释放对此缓冲区的访问
     */
    void unpin() {
        pins--;
    }

    /**
     * 判断当前缓冲区是否正在被事务访问
     *
     *
     * @return
     * 标识是否被占用的布尔值
     */
    boolean isPinned() {
        return pins > 0;
    }

    /**
     * 将缓冲区内容写回物理块中
     */
    void flush() {
        if (modifiedBy >= 0) {
            LogManager lMg = SimpleDB.logManager();
            lMg.flush(logSequenceNum);

            contents.write(myBlk);
        }
    }

    /**
     * 将指定物理块调入此内存缓冲区
     *
     *
     * @param block
     * 指定的物理块
     */
    void assignToBlock(Block block) {
        flush();
        myBlk = block;
        contents.read(myBlk);
        pins = 0;
    }

    /**
     * 为指定文件追加物理块，并将物理块格式化后调入此缓冲区
     *
     *
     * @param filename
     * 文件名
     *
     * @param pfm
     * 格式化方法
     */
    void assignToNew(String filename, PageFormatter pfm) {
        flush();
        pfm.format(contents);
        myBlk = contents.append(filename);
        pins = 0;
    }
}
