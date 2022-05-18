package simpledb.log;

import simpledb.file.Block;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * 日志记录迭代器
 *
 *
 * @author shs
 * @date 2022/5/18 20:55
 */
public class LogIterator implements Iterable<BasicLogRecord> {
    private Block currentBlock;

    LogIterator(Block b) {
        currentBlock = b;
    }

    @Override
    public Iterator<BasicLogRecord> iterator() {
        return null;
    }

    @Override
    public void forEach(Consumer<? super BasicLogRecord> action) {
        Iterable.super.forEach(action);
    }

    @Override
    public Spliterator<BasicLogRecord> spliterator() {
        return Iterable.super.spliterator();
    }
}
