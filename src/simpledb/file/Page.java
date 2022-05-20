package simpledb.file;
import simpledb.server.SimpleDB;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * 实现了对内存页的数据存取；
 * 实现了内存页与物理块之间的数据交换；
 *
 *
 * @author shs
 * @Data: 2022/5/16 21:00
 */
public class Page {
    public static final int BLOCK_SIZE = 400;
    public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
    public static final int STR_SIZE(int n) {
        // 机器字符集中char的字节数
        float bytesPerChar = Charset.defaultCharset().newEncoder().maxBytesPerChar();
        return n * (int)bytesPerChar + INT_SIZE;
    }
    private ByteBuffer content = ByteBuffer.allocateDirect(BLOCK_SIZE);
    private FileManager fmg = SimpleDB.fileManager();


    /**
     * 向缓冲区内存入一个整数
     *
     * @param offset
     * 待存入数据的块(页)内偏移量
     *
     * @param val
     * 待存入的整数
     */
    public synchronized void setInt(int offset, int val) {
        content.putInt(offset, val);
    }

    /**
     * 从缓冲区内取出一个整数
     *
     * @param offset
     * 待取出数据的块(页)内偏移量
     *
     * @return
     * 取出的整数
     */
    public synchronized int getInt(int offset) {
        return content.getInt(offset);
    }

    /**
     * 向缓冲区内存入字符串：字符串字节数 + 字符串
     *
     * @param offset
     * 待存入数据的块(页)内偏移量
     *
     * @param val
     * 待存入的字符串
     */
    public synchronized void setString(int offset, String val) {
        content.position(offset);
        byte[] byteVal = val.getBytes();

        content.putInt(byteVal.length);
        content.put(byteVal);
    }

    /**
     * 从缓冲区内取出字符串
     *
     * @param offset
     * 待取出数据的块(页)内偏移量
     *
     * @return
     * 取出的字符串
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
     *
     * @param block
     * 待写入的目标物理块
     */
    public synchronized void write(Block block) {
        fmg.write(content, block);
    }

    /**
     * 将指定物理块内的数据读入缓冲区
     *
     * @param block
     * 存放待读入数据的物理块
     */
    public synchronized void read(Block block) {
        fmg.read(content, block);
    }

    /**
     * 将缓冲区内的数据追加到指定文件的最后一个物理块
     *
     * @param filename
     * 待追加数据文件的文件名
     *
     * @return
     * 文件最后一个物理块引用
     */
    public synchronized Block append(String filename) {
        return fmg.append(content, filename);
    }
}
