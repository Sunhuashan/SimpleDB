package simpledb.file;

import java.util.Objects;

/**
 * @Author: shs
 * @Data: 2022/5/16 20:44
 *
 *
 * Block类使用文件名与块号(文件逻辑块号)唯一标识物理块
 */
public class Block {
    private String filename;
    private int blkNum;

    Block(String filename, int blkNum) {
        this.filename = filename;
        this.blkNum = blkNum;
    }

    public String getFilename() {
        return filename;
    }

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
