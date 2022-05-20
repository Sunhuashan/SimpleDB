package simpledb.log;

import simpledb.file.Page;

import static simpledb.file.Page.*;

/**
 * 一个BasicLogRecord对象对应一条日志记录
 * <p>
 * 日志记录以 Object[] 形式存在，目前支持：Integer、String对象
 * </p>
 *
 *
 * @author shs
 * @date 2022/5/18 16:50
 */
public class BasicLogRecord {

    private Page logPage;
    private int pos;

    /**
     * 用内存页面和页内偏移位置构造日志记录对象
     *
     *
     * @param page
     * 内存缓冲区页面
     *
     * @param position
     * 日志记录的起始位置的页内偏移量
     */
    public BasicLogRecord(Page page, int position) {
        this.logPage = page;
        this.pos = position;
    }

    /**
     * 获取日志记录中的一个整数
     *
     *
     * @return
     * 日志记录中的整数
     */
    public int nextInt() {
        int val = logPage.getInt(pos);
        pos += INT_SIZE;
        return val;
    }

    /**
     * 获取日志记录中的一个字符串
     *
     *
     * @return
     * 日志记录中的字符串
     */
    public String nextString() {
        String val = logPage.getString(pos);
        pos += STR_SIZE(val.length());
        return val;
    }
}
