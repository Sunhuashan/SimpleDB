package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferManager;
import simpledb.buffer.PageFormatter;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  除了提供 pin/unpin 接口外，该类还保存了当前事务拥有的缓冲区
 *  以及该事务固定的物理块(物理块对象个数与固定次数相同)
 *  <p>向事务提供的 pin/unpin 接口隐藏了缓冲区的存在，对事务而言，
 *  可以重复 pin 同一个物理块，但每次获取的是同一个缓冲区
 */
public class BufferList {
    //事务 pin 的物理块，物理块对象个数与固定次数相同
    private List<Block> blocks = new ArrayList<>();
    //存储物理块对应的缓冲区
    private Map<Block, Buffer> buffers = new HashMap<>();
    //缓冲区管理对象
    private BufferManager bmg = SimpleDB.bufferManager();

    /**
     * 获取指定物理块的缓冲区
     *
     *
     * @param blk
     * 指定物理块
     *
     * @return
     * 缓冲区
     */
    Buffer getBuffer(Block blk) {
        return buffers.get(blk);
    }

    /**
     * 固定指定物理块
     *
     *
     * @param blk
     * 指定物理块
     */
    void pin(Block blk) {
        Buffer buffer = bmg.pin(blk);
        buffers.put(blk, buffer);
        blocks.add(blk);
    }

    /**
     * 固定新的物理块(指定文件的新的追加块)
     *
     *
     * @param filename
     * 指定的文件名
     *
     * @param fmt
     * 物理块格式化类
     *
     * @return
     * 存放物理块的缓冲区
     */
    Buffer pinNew(String filename, PageFormatter fmt) {
        Buffer buffer = bmg.pinNew(filename, fmt);
        buffers.put(buffer.block(), buffer);
        blocks.add(buffer.block());
        return buffer;
    }

    /**
     * 取消固定指定物理块
     *
     *
     * @param blk
     * 指定物理块
     */
    void unpin(Block blk) {
        Buffer buffer = this.getBuffer(blk);
        bmg.unpin(buffer);
        blocks.remove(blk);
        if (!blocks.contains(blk))
            buffers.remove(blk);
    }

    /**
     * 取消固定此事务固定的所有物理块
     */
    void unpinAll() {
        for (Block block : blocks)
            unpin(block);
        buffers.clear();
        blocks.clear();
    }
}
