package simpledb.tx.concurrency;

import simpledb.exception.LockAbortException;
import simpledb.file.Block;

import java.util.HashMap;
import java.util.Map;

/**
 * 数据库中的事务共享一个LockTable对象，该对象维护了各个物理块的加锁情况
 */
public class LockTable {
    // 10s
    private static final long MAX_TIME = 10000;
    private Map<Block, Integer> locks = new HashMap<>();

    /**
     * 申请指定物理块的 s-lock
     *
     * @param blk
     * 指定物理块
     */
    public synchronized void sLock(Block blk) {
        long timestamp = System.currentTimeMillis();
        try {
            while (hasXLock(blk) && !waitToLong(timestamp))
                wait(MAX_TIME);
            if (hasXLock(blk))
                throw new LockAbortException();
            int val = getValue(blk);
            locks.put(blk, val + 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new LockAbortException();
        }
    }

    /**
     * 申请指定物理块的 x-lock
     *
     * @param blk
     * 指定物理块
     */
    public synchronized void xLock(Block blk) {
        long timestamp = System.currentTimeMillis();
        try {
            while (hasOtherSLock(blk) && !waitToLong(timestamp))
                wait(MAX_TIME);
            if (hasOtherSLock(blk))
                throw new LockAbortException();
            locks.put(blk, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new LockAbortException();
        }
    }

    /**
     * 释放对指定物理块的锁
     *
     * @param blk
     * 指定物理块
     */
    public synchronized void unlock(Block blk) {
        int val = getValue(blk);
        if (val > 1)
            locks.put(blk, val - 1);
        else {
            locks.remove(blk);
            notifyAll();
        }
    }

    /**
     * 判断指定物理块是否被 x-lock 锁定
     *
     *
     * @param blk
     * 指定物理块
     *
     * @return
     * true or false
     */
    boolean hasXLock(Block blk) {
        return getValue(blk) < 0;
    }

    /**
     * 事务在获取 x-lock 前会先获取 s-lock
     * 断物理块是否被其他事务的 s-lock 锁定
     *
     *
     * @param blk
     * 指定物理块
     *
     * @return
     * true or false
     */
    boolean hasOtherSLock(Block blk) {
        return getValue(blk) > 1;
    }

    /**
     * 获取指定物理块的锁定值
     *
     *
     * @param blk
     * 指定物理块
     *
     * @return
     * 锁定值
     */
    int getValue(Block blk) {
        Integer val = locks.get(blk);
        if (null == val)
            return 0;
        return val;
    }

    boolean waitToLong(long timestamp) {
        return System.currentTimeMillis() - timestamp > MAX_TIME;
    }
}

