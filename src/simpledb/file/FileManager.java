package simpledb.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * FileManager类实现了内存缓冲区与物理块之间的数据交换
 *
 *
 * @author shs
 * @date 2022/5/17 10:06
 */
public class FileManager {
    public static final String HOME_DIR = System.getProperty("user.home");
    private File dbDirectory;
    private boolean isNew;
    private Map<String, RandomAccessFile> openedFiles = new HashMap<>();


    /**
     * 带参构造函数，创建数据库根目录
     *
     * @param dbName
     * 数据库名称
     */
    public FileManager(String dbName) {
        dbDirectory = new File(HOME_DIR, dbName);
        isNew = !dbDirectory.exists();

        //创建数据库目录
        if (isNew) {
            if (!dbDirectory.mkdirs()) {
                throw new RuntimeException("Cannot create directory" + dbDirectory);
            }
        }

        //删除临时文件
        for (String filename : dbDirectory.list()) {
            if (filename.startsWith("temp"))
                new File(dbDirectory, filename).delete();
        }
    }

    /**
     * 判断数据库是否已存在
     *
     * @return
     * 表示数据库是否为新的 boolean 型变量
     */
    public boolean isNew() {
        return isNew;
    }

    /**
     * 根据文件名获取文件块数
     *
     * @param filename
     * 文件名
     *
     * @return
     * 文件块数
     *
     * @throws IOException
     * If some I/O error occurs
     */
    public int size(String filename) throws IOException{
        FileChannel fc = getFile(filename).getChannel();
        return (int) fc.size() / Page.BLOCK_SIZE;
    }

    /**
     * 根据文件名返回文件读写流
     *
     * @param filename
     * 文件名
     *
     * @return
     * 文件读写流 RandomAccessFile
     *
     * @throws IOException
     * If some I/O error occurs
     */
    private synchronized RandomAccessFile getFile(String filename) throws IOException {
        RandomAccessFile raf = openedFiles.get(filename);
        if (raf == null) {
            File f = new File(dbDirectory, filename);
            raf = new RandomAccessFile(f, "rws");
            openedFiles.put(filename, raf);
        }
        return raf;
    }

    /**
     * 将缓冲区内的数据写入到指定物理块
     *
     * @param buffer
     * 存放待写入数据的缓冲区
     *
     * @param block
     * 数据写入的目标物理块
     */
    synchronized void write(ByteBuffer buffer, Block block) {
        try {
            buffer.rewind();
            FileChannel fc = getFile(block.getFilename()).getChannel();
            fc.write(buffer, Page.BLOCK_SIZE * block.getBlkNum());
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot write block" + block.toString());
        }
    }


    /**
     * 将指定物理块内的数据读取到缓冲区
     *
     * @param buffer
     * 数据读入的目标缓冲区
     *
     * @param block
     * 存放待读取数据的物理块
     */
    synchronized void read(ByteBuffer buffer, Block block) {
        try {
            buffer.clear();
            FileChannel fc = getFile(block.getFilename()).getChannel();
            fc.read(buffer, Page.BLOCK_SIZE * block.getBlkNum());
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot read block" + block.toString());
        }
    }

    /**
     * 将缓冲区内的数据追加到指定文件的最后一个物理块
     *
     * @param buffer
     * 存放待写入数据的缓冲区
     *
     * @param filename
     * 文件名
     *
     * @return
     * 文件最后一个物理块的引用
     */
    synchronized Block append(ByteBuffer buffer, String filename) {
        try {
            Block newBlk = new Block(filename, size(filename));
            write(buffer, newBlk);
            return newBlk;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot append to file :" + filename);
        }
    }
}
