package simpledb.buffer;

import simpledb.exception.BufferAbortException;
import simpledb.file.Block;

/**
 * 封装BasicBufferManager类，维护事务等待队列
 * <p>
 * 当事务想要访问指定物理块，但缓冲区无空余时。缓冲管理器会将
 * 事务放入一个等待队列中，待其他事务释放缓冲区后，在依次为等
 * 待队列中的事务申请缓冲区。若事务等待超过指定时间后，则认为
 * 系统进入死锁状态，进行回滚操作，解除死锁。
 * </p>
 *
 *
 * @author shs
 * @date 2022/5/22 19:12
 */
public class BufferManager {

    //每个事务的最长等待时间 10s
    private static final int MAX_TIME = 10000;

    private BasicBufferManager basicBufferMgr;

    public BufferManager(BasicBufferManager bbm) {
        basicBufferMgr = bbm;
    }

    public synchronized Buffer pin(Block block) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buffer = basicBufferMgr.pin(block);
            while (null == buffer && !waitTooLong(timestamp)) {
                wait(MAX_TIME);
                buffer = basicBufferMgr.pin(block);
            }
            //等待时间超出阈值仍未获取到Buffer对象，抛出异常由调用者处理
            if (null == buffer) {
                throw new BufferAbortException();
            }
            return buffer;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new BufferAbortException();
        }
    }

    public synchronized Buffer pinNew(String filename, PageFormatter pfm) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buffer = basicBufferMgr.pinNew(filename, pfm);
            while (null == buffer && !waitTooLong(timestamp)) {
                wait(MAX_TIME);
                buffer = basicBufferMgr.pinNew(filename, pfm);
            }
            if (null == buffer) {
                throw new BufferAbortException();
            }
            return buffer;
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new BufferAbortException();
        }
    }

    public synchronized void unpin(Buffer buffer) {
        basicBufferMgr.unpin(buffer);
        if (!buffer.isPinned())
            notifyAll();
    }

    public void flushAll(int txNum) {
        basicBufferMgr.flushAll(txNum);
    }

    public int available() {
        return basicBufferMgr.available();
    }

    boolean waitTooLong(long timestamp) {
        return System.currentTimeMillis() - timestamp > MAX_TIME;
    }
}
