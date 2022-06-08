package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.PageFormatter;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.concurrency.ConcurrencyManager;
import simpledb.tx.recovery.RecoveryManager;

import java.io.IOException;

/**
 * 事务是数据库系统的基本单位，该类是对事务的抽象
 */
public class Transaction {
   //静态变量，方便每个事务对象获取不同的事务号
   private static int nextTxNum = 0;
   //事务号
   private int txNum;
   //恢复管理器 管理日志记录
   private RecoveryManager rmg;
   //并发管理器 负责物理块的互斥访问
   private ConcurrencyManager cmg;
   //该事务固定的缓冲区
   private BufferList buffers = new BufferList();


   /**
    * 无参构造方法
    */
   public Transaction() {
      txNum = nextTxNum();
      rmg = new RecoveryManager(txNum);
      cmg = new ConcurrencyManager();
   }

   /**
    * 提交事务
    * <p>事务释放其固定的缓冲区以及其加锁的物理块后提交
    */
   public void commit() {
      buffers.unpinAll();
      cmg.release();
      rmg.commit();
      System.out.println("Transaction " + txNum + " committed");
   }

   /**
    * 回滚事务
    * <p>事务释放其固定的缓冲区以及其加锁的物理块后回滚
    */
   public void rollback() {
      buffers.unpinAll();
      cmg.release();
      rmg.rollback();
      System.out.println("Transaction " + txNum + " rolled back");
   }

   /**
    * 数据库恢复
    */
   public void recovery() {
      SimpleDB.bufferManager().flushAll(txNum);
      rmg.recovery();
   }

   /**
    * 固定指定的物理块
    *
    *
    * @param block
    * 指定的物理块
    */
   public void pin(Block block) {
      buffers.pin(block);
   }

   /**
    * 取消固定指定物理块
    *
    *
    * @param block
    * 指定物理块
    */
   public void unpin(Block block) {
      buffers.unpin(block);
   }

   /**
    * 向物理块指定位置写入一个整数
    *
    *
    * @param block
    * 指定物理块
    *
    * @param offset
    * 块内偏移
    *
    * @param val
    * 整数
    */
   public void setInt(Block block, int offset, int val) {
      cmg.xLock(block);
      Buffer buffer = buffers.getBuffer(block);
      int lsn = rmg.setIntRec(buffer, offset, val);
      buffer.setInt(offset, val, txNum, lsn);
   }

   /**
    * 获取一个整数
    * @param block
    * @param offset
    * @return
    */
   public int getInt(Block block, int offset) {
      cmg.sLock(block);
      Buffer buffer = buffers.getBuffer(block);
      return buffer.getInt(offset);
   }

   /**
    * 向物理块的指定位置写入一个字符串
    * @param block
    * @param offset
    * @param val
    */
   public void setString(Block block, int offset, String val) {
      cmg.xLock(block);
      Buffer buffer = buffers.getBuffer(block);
      int lsn = rmg.setStringRec(buffer, offset, val);
      buffer.setString(offset, val, txNum, lsn);
   }

   /**
    * 获取一个字符串
    * @param block
    * @param offset
    * @return
    */
   public String getString(Block block, int offset) {
      cmg.sLock(block);
      Buffer buffer = buffers.getBuffer(block);
      return buffer.getString(offset);
   }

   /**
    * 获取指定文件的物理块数
    *
    *
    * @param filename
    * 文件名
    *
    * @return
    * 物理块数量
    *
    * @throws IOException
    * 读写异常
    */
   public int size(String filename) throws IOException {
      Block endOfFile = new Block(filename, -1);
      cmg.sLock(endOfFile);
      return SimpleDB.fileManager().size(filename);
   }

   /**
    * 向指定文件追加一个物理块
    * @param filename
    * 文件名
    *
    * @param fmt
    * 文件格式化对象
    *
    * @return
    * 追加的物理块对象
    */
   public Block append(String filename, PageFormatter fmt) {
      Block endOfFile = new Block(filename, -1);
      cmg.xLock(endOfFile);

      Block block = buffers.pinNew(filename, fmt);
      unpin(block);
      return block;
   }

   public int getTxNum() {
      return txNum;
   }

   private static synchronized int nextTxNum() {
      ++nextTxNum;
      System.out.println("new transaction number is " + nextTxNum);
      return nextTxNum;
   }
}
