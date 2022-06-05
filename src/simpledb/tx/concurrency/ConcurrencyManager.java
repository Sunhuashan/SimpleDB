package simpledb.tx.concurrency;

import simpledb.file.Block;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyManager {
    private static LockTable lockTbl = new LockTable();
    private Map<Block, String> locks = new HashMap<>();


    public void sLock(Block blk) {
        if (null == locks.get(blk)) {
            locks.put(blk, "S");
            lockTbl.sLock(blk);
        }
    }

    public void xLock(Block blk) {
        if (!hasXLock(blk)) {
            sLock(blk);
            locks.put(blk, "X");
            lockTbl.xLock(blk);
        }
    }

    public void release() {
        for (Block block : locks.keySet())
            lockTbl.unlock(block);
        locks.clear();
    }

    boolean hasXLock(Block blk) {
        String type = locks.get(blk);
        return (type != null) && type.equals("X");
    }
}
