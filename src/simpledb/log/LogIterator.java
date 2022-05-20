package simpledb.log;

import simpledb.file.Block;
import simpledb.file.Page;

import java.util.Iterator;

import static simpledb.file.Page.INT_SIZE;
import static simpledb.log.LogManager.LAST_POS;

/**
 * 日志记录迭代器
 * <p>
 * 从指定块末尾开始，逆向遍历整个日志文件
 * </p>
 *
 *
 * @author shs
 * @date 2022/5/18 20:55
 */
public class LogIterator implements Iterator<BasicLogRecord> {

    private Block currentBlk;
    private Page page = new Page();
    private int currentRec;

    /**
     * 为指定物理块中的日志记录构造一个迭代器
     *
     *
     * @param block
     * 指定的物理块
     */
    LogIterator(Block block) {
        currentBlk = block;
        page.read(block);
        currentRec = page.getInt(LAST_POS);
    }

    @Override
    public boolean hasNext() {
        return currentRec > 0 || currentBlk.getBlkNum() >= 0;
    }

    @Override
    public BasicLogRecord next() {
        if (0 == currentRec)
            removeToNextBlk();
        currentRec = page.getInt(currentRec);
        return new BasicLogRecord(page, currentRec + INT_SIZE);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot execute this operation for log record");
    }

    /**
     * 当前块迭代完毕后，移至上一块
     */
    private void removeToNextBlk() {
        currentBlk = new Block(currentBlk.getFilename(), currentBlk.getBlkNum() - 1);
        page.read(currentBlk);
        currentRec = page.getInt(LAST_POS);
    }
}
