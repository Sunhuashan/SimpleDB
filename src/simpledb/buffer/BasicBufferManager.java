package simpledb.buffer;

import jdk.nashorn.internal.runtime.regexp.joni.ast.StringNode;
import simpledb.file.Block;

/**
 * 负责管理缓冲池
 *
 *
 * @author shs
 * @date 2022/5/22 16:04
 */
public class BasicBufferManager {

    private Buffer[] bufferPool;
    private int availableNum;


    /**
     * 构造指定大小的缓冲池
     *
     *
     * @param poolSize
     * 指定缓冲池大小
     */
    public BasicBufferManager(int poolSize) {
        availableNum = poolSize;
        bufferPool = new Buffer[poolSize];
        for (int i = 0; i < poolSize; i++)
            bufferPool[i] = new Buffer();
    }

    /**
     * 将指定事务标识访问的缓冲区写回物理块
     *
     *
     * @param txNum
     * 指定的事务号
     */
    public synchronized void flushAll(int txNum) {
        for (int i = 0; i < bufferPool.length; i++)
            if (bufferPool[i].isModifiedBy(txNum))
                bufferPool[i].flush();
    }

    /**
     * 标识事务对存放指定物理块的缓冲区的访问
     *
     *
     * @param blk
     * 指定的物理块
     *
     * @return
     * 缓冲区对象
     */
    public synchronized Buffer pin(Block blk) {
        Buffer buffer = existBuffer(blk);
        if (null == buffer) {
            buffer = chooseUnpinnedBuffer();

            //调用者进入wait()状态
            if (null == buffer)
                return null;

            buffer.assignToBlock(blk);
        }
        if (!buffer.isPinned())
            availableNum--;
        buffer.pin();
        return buffer;
    }

    /**
     * 标识事务对存放指定文件最后一个物理块的缓冲区的访问
     *
     *
     * @param filename
     * 文件名
     *
     * @param pfm
     * 物理块格式化方法
     *
     * @return
     * 缓冲区对象
     */
    public synchronized Buffer pinNew(String filename, PageFormatter pfm) {
        Buffer buffer = chooseUnpinnedBuffer();
        if (null == buffer)
            return null;
        buffer.assignToNew(filename, pfm);
        buffer.pin();
        availableNum--;
        return buffer;
    }

    /**
     * 取消对指定缓冲区的访问标识
     *
     *
     * @param buffer
     * 缓冲区对象
     */
    public synchronized void unpin(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned())
            availableNum++;
    }

    /**
     * 获取缓冲池中未被标识访问的缓冲区个数
     *
     *
     * @return
     * 空闲的缓冲区个数
     */
    public int available() {
        return availableNum;
    }

    /**
     * 获取存放指定物理块且已经被标识访问的缓冲区
     *
     *
     * @param blk
     * 指定物理块对象
     *
     * @return
     * 目标缓冲区对象或null
     */
    Buffer existBuffer(Block blk) {
        for (Buffer buffer : bufferPool) {
            Block bufferBlk = buffer.block();
            if (bufferBlk != null && bufferBlk.equals(blk))
                return buffer;
        }
        return null;
    }

    /**
     * 在所有空闲缓冲区中按照某种算法寻找可以“对换”的缓冲区
     * <p>
     * 当前使用最简单的顺序遍历算法
     * </p>
     * @return
     * 合适的缓冲区
     */
    Buffer chooseUnpinnedBuffer() {
        for (Buffer buffer : bufferPool) {
            if (!buffer.isPinned())
                return buffer;
        }
        return null;
    }
}

