package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @Author: shs
 * @Data: 2022/5/16 21:00
 *
 *
 * Page类实现了对内存页的数据存取；
 * 实现了内存页与物理块之间的数据交换；
 */
public class Page {
    public static final int BLOCK_SIZE = 400;
    public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

    public static final int STR_SIZE(int n) {
        // 机器字符集中char的字节数
        float bytesPerChar = Charset.defaultCharset().newEncoder().maxBytesPerChar();
        return n * (int)bytesPerChar;
    }

    private ByteBuffer content = ByteBuffer.allocateDirect(BLOCK_SIZE);
    private FileManager fm = new FileManager();
    /**
     * 向缓冲区内存入一个整数
     * @param offset
     * @param val
     */
    public synchronized void setInt(int offset, int val) {
        content.putInt(offset, val);
    }

    /**
     * 从缓冲区内取出一个整数
     * @param offset
     * @return
     */
    public synchronized int getInt(int offset) {
        return content.getInt(offset);
    }

    /**
     * 向缓冲区内存入字符串：字符串字节数 + 字符串
     * @param offset
     * @param val
     */
    public synchronized void setString(int offset, String val) {
        content.position(offset);
        byte[] byteVal = val.getBytes();

        content.putInt(byteVal.length);
        content.put(byteVal);
    }

    /**
     * 从缓冲区内取出字符串
     * @param offset
     * @return
     */
    public synchronized String getString(int offset) {
        content.position(offset);
        int len = content.getInt();

        byte[] bytes = new byte[len];
        content.get(bytes);
        return new String(bytes);
    }

    /**
     * 将缓冲区内的数据写入到指定物理块
     * @param block
     */
    public synchronized void write(Block block) {
        fm.write(content, block);
    }

    /**
     * 将指定物理块内的数据写入缓冲区
     * @param block
     */
    public synchronized void read(Block block) {
        fm.read(content, block);
    }

    /**
     * 将缓冲区内的数据追加到指定文件的最后一个物理块，
     * 并返回物理块的引用
     * @param filename
     * @return
     */
    public synchronized Block append(String filename) {
        return fm.append(content, filename);
    }
}
