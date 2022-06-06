package simpledb.tx;

import simpledb.buffer.PageFormatter;
import simpledb.file.Block;
import simpledb.tx.concurrency.ConcurrencyManager;
import simpledb.tx.recovery.RecoveryManager;

public class Transaction {
   private static int nextTxNum = 0;
   private int txNum;
   private RecoveryManager rmg;
   private ConcurrencyManager cmg;


   public Transaction() {
      txNum = nextTxNum();
      rmg = new RecoveryManager(txNum);
      cmg = new ConcurrencyManager();
   }

   public void commit() {

   }
   public void rollback() {

   }
   public void recovery() {

   }

   public void pin(Block block) {

   }
   public void unpin(Block block) {

   }
   public void setInt(Block block, int offset, int val) {

   }
   public int getInt(Block block, int offset) {

   }
   public void setString(Block block, int offset, String val) {

   }
   public String getString(Block block, int offset) {

   }

   public int size(String filename) {

   }
   public Block append(String filename, PageFormatter fmt) {

   }

   private static synchronized int nextTxNum() {
      ++nextTxNum;
      System.out.println("new transaction number is " + nextTxNum);
      return nextTxNum;
   }
}
