package simpledb.file;

import simpledb.server.SimpleDB;

/**
 * 文件管理功能单元测试
 *
 *
 * @author shs
 * @date 2022/5/18 11:27
 */
public class FileTest {
    public static void main(String[] args) {
        //创建数据库目录，以及该目录的File Manager对象
        SimpleDB.init("first_db");
        //创建Page对象: 内存缓冲区
        Page p1 = new Page();
        //创建Block对象：物理块引用
        Block b1 = new Block("stuTbl", 1);

        //向物理块写入内存缓冲区中的数据
        p1.setInt(0, 20);
        p1.setString(16, "Hello,SimpleDB");
        p1.setInt(396, 0xff);
        p1.write(b1);

        //从物理块中读取数据至内存缓冲区,打印
        p1 = new Page();
        p1.read(b1);
        int n = p1.getInt(0);
        String s = p1.getString(16);
        System.out.println("test int value: " + n);
        System.out.println("test String value: " + s);

        //将缓冲区中的数据追加至文件最后一个物理块
        p1 = new Page();
        p1.setInt(0, 20);
        p1.setString(16, "Hello,SimpleDB");
        p1.setInt(396, 0xff);
        Block block = p1.append("stuTbl");
        System.out.println("The last block is: " + block.toString());


    }
}
