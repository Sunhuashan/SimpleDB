package simpledb.file;

import java.util.Objects;

/**
 * 使用文件名与块号(文件逻辑块号)唯一标识物理块
 *
 *
 * @Author: shs
 * @Data: 2022/5/16 20:44
 */
public class Block {
    private String filename;
    private int blkNum;

    /**
     * 使用文件名与文件块号构造物理块的引用，即 Block 对象
     *
     * @param filename
     * 文件名
     *
     * @param blkNum
     * 文件块号
     */
    public Block(String filename, int blkNum) {
        this.filename = filename;
        this.blkNum = blkNum;
    }

    /**
     * 获取 Block 对象所属文件的文件名
     *
     * @return
     * 文件名
     */
    public String getFilename() {
        return filename;
    }

    /**
     * 获取 Block 对象所属文件的块号
     *
     * @return
     * 块号
     */
    public int getBlkNum() {
        return blkNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Block block = (Block) o;
        return blkNum == block.blkNum && Objects.equals(filename, block.filename);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return "Block{" +
                "filename='" + filename + '\'' +
                ", blkNum=" + blkNum +
                '}';
    }
}
