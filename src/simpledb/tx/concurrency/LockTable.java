package simpledb.tx.concurrency;

import simpledb.exception.LockAbortException;
import simpledb.file.Block;

import java.util.HashMap;
import java.util.Map;

public class LockTable {
    // 10s
    private static final long MAX_TIME = 10000;
    private Map<Block, Integer> locks = new HashMap<>();


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

    public synchronized void unlock(Block blk) {
        int val = getValue(blk);
        if (val > 1)
            locks.put(blk, val - 1);
        else {
            locks.remove(blk);
            notifyAll();
        }
    }

    boolean hasXLock(Block blk) {
        return getValue(blk) < 0;
    }

    /**
     * 事务在获取 x-lock 前会先获取 s-lock
     * 该方法判断物理块是否有其他事务的 s-lock
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

