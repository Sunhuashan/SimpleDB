package simpledb.tx.concurrency;

import simpledb.file.Block;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyManager {
    //所有事务的共享锁表
    private static LockTable lockTbl = new LockTable();
    //当前事务所持有的锁表
    private Map<Block, String> locks = new HashMap<>();


    /**
     * 为事务请求指定块的共享锁 事务不拥有该块任何锁时才能请求成功
     *
     *
     * @param blk
     * 指定的块
     */
    public void sLock(Block blk) {
        if (null == locks.get(blk)) {
            locks.put(blk, "S");
            lockTbl.sLock(blk);
        }
    }

    /**
     * 为事务请求指定块的互斥锁
     * <p>
     * 规定事务拥有指定块的共享锁后才能获取该块的互斥锁
     *
     *
     * @param blk
     * 指定的块
     */
    public void xLock(Block blk) {
        if (!hasXLock(blk)) {
            sLock(blk);
            locks.put(blk, "X");
            lockTbl.xLock(blk);
        }
    }

    /**
     * 释放事务拥有的所有锁
     */
    public void release() {
        for (Block block : locks.keySet())
            lockTbl.unlock(block);
        locks.clear();
    }

    /**
     * 判断事务是否拥有指定块的互斥锁
     *
     *
     * @param blk
     * 指定的块
     *
     * @return
     * true or false
     */
    boolean hasXLock(Block blk) {
        String type = locks.get(blk);
        return (type != null) && type.equals("X");
    }
}
