package simpledb.tx;

import jdk.nashorn.internal.ir.BlockLexicalContext;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

import javax.xml.crypto.dsig.TransformService;

/*
Thread A: sLock(b1); sLock(b2); unlock(b1); unlock(b2);
Thread B: xLock(b2); sLock(b1); unlock(b1); unlock(b2);
Thread C: xLock(b1); sLock(b2); unlock(b1); unlock(b2);
 */
public class TxTest {


    public static void main(String[] args) {
        SimpleDB.init("stuDB");
        TestA testA = new TestA();
        TestB testB = new TestB();
        TestC testC = new TestC();

        new Thread(testA).start();
        new Thread(testB).start();
        new Thread(testC).start();
    }

}
class TestA implements Runnable {

    @Override
    public void run() {
        Transaction tx = new Transaction();
        System.out.println("Tx A --> txNum = " + tx.getTxNum());
        String filename = "junk";
        Block b0 = new Block(filename, 0);
        Block b1 = new Block(filename, 1);
        Block b2 = new Block(filename, 2);

        System.out.println("Tx A start read b1");
        tx.pin(b1);
        String val1 = tx.getString(b1, 100);
        System.out.println("Tx A the string val in b1 is " + val1);
        System.out.println("Tx A end read b1");

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Tx A start read b2");
        tx.pin(b2);
        String val2 = tx.getString(b2, 100);
        System.out.println("Tx A the string val in b2 is " + val2);
        System.out.println("Tx A end read b2");
        tx.commit();
    }
}

class TestB implements Runnable{
    @Override
    public void run() {
        Transaction tx = new Transaction();
        System.out.println("Tx B --> txNum = " + tx.getTxNum());
        String filename = "junk";
        Block b0 = new Block(filename, 0);
        Block b1 = new Block(filename, 1);
        Block b2 = new Block(filename, 2);

        System.out.println("Tx B start write b2");
        tx.pin(b2);
        tx.setString(b2, 100, "B");
        System.out.println("Tx B the string val set in b2 is B");
        System.out.println("Tx B end read b2");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("tx B start read b1");
        tx.pin(b1);
        String val = tx.getString(b1, 100);
        System.out.println("Tx B the string val in b1 is " + val);
        System.out.println("Tx B end read b1");
        tx.commit();
    }
}
class TestC implements Runnable {

    @Override
    public void run() {
        Transaction tx = new Transaction();
        System.out.println("Tx C --> txNum = " + tx.getTxNum());
        String filename = "junk";
        Block b0 = new Block(filename, 0);
        Block b1 = new Block(filename, 1);
        Block b2 = new Block(filename, 2);

        System.out.println("Tx C start write b1");
        tx.pin(b1);
        tx.setString(b1, 100, "C");
        System.out.println("Tx C the string val set in b1 is C");
        System.out.println("Tx C end write b1");

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Tx C start read b2");
        tx.pin(b2);
        String val = tx.getString(b2, 100);
        System.out.println("Tx C the string val in b2 is " + val);
        System.out.println("Tx C end read b2");
        tx.commit();

    }
}