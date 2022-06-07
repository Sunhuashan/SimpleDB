package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.PageFormatter;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.concurrency.ConcurrencyManager;
import simpledb.tx.recovery.RecoveryManager;

import java.io.IOException;

public class Transaction {
   private static int nextTxNum = 0;
   private int txNum;
   private RecoveryManager rmg;
   private ConcurrencyManager cmg;
   private BufferList buffers = new BufferList();


   public Transaction() {
      txNum = nextTxNum();
      rmg = new RecoveryManager(txNum);
      cmg = new ConcurrencyManager();
   }

   public void commit() {
      cmg.release();
      rmg.commit();
      buffers.unpinAll();
      System.out.println("Transaction " + txNum + " committed");
   }
   public void rollback() {
      cmg.release();
      rmg.rollback();
      buffers.unpinAll();
      System.out.println("Transaction " + txNum + " rolled back");
   }
   public void recovery() {
      SimpleDB.bufferManager().flushAll(txNum);
      rmg.recovery();
   }
   public void pin(Block block) {
      buffers.pin(block);
   }
   public void unpin(Block block) {
      buffers.unpin(block);
   }
   public void setInt(Block block, int offset, int val) {
      cmg.xLock(block);
      Buffer buffer = buffers.getBuffer(block);
      int lsn = rmg.setIntRec(buffer, offset, val);
      buffer.setInt(offset, val, txNum, lsn);
   }
   public int getInt(Block block, int offset) {
      cmg.sLock(block);
      Buffer buffer = buffers.getBuffer(block);
      return buffer.getInt(offset);
   }
   public void setString(Block block, int offset, String val) {
      cmg.xLock(block);
      Buffer buffer = buffers.getBuffer(block);
      int lsn = rmg.setStringRec(buffer, offset, val);
      buffer.setString(offset, val, txNum, lsn);
   }
   public String getString(Block block, int offset) {
      cmg.sLock(block);
      Buffer buffer = buffers.getBuffer(block);
      return buffer.getString(offset);
   }

   public int size(String filename) throws IOException {
      Block endOfFile = new Block(filename, -1);
      cmg.xLock(endOfFile);
      return SimpleDB.fileManager().size(filename);
   }
   public Block append(String filename, PageFormatter fmt) {
      Block endOfFile = new Block(filename, -1);
      cmg.xLock(endOfFile);

      Block block = buffers.pinNew(filename, fmt);
      unpin(block);
      return block;
   }

   private static synchronized int nextTxNum() {
      ++nextTxNum;
      System.out.println("new transaction number is " + nextTxNum);
      return nextTxNum;
   }
}
