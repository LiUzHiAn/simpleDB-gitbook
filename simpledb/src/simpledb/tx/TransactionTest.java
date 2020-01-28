package simpledb.tx;


import simpledb.buffer.ABCStringFormatter;
import simpledb.buffer.PageFormatter;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

import java.awt.print.PageFormat;

/**
 * @ClassName TransactionTest
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-19 19:32
 * @Version 1.0
 **/

public class TransactionTest {

    public static void main(String[] args) {
        SimpleDB.init("studentdb");
        TestA tA = new TestA();new Thread(tA).start();
        TestB tB = new TestB();new Thread(tB).start();
        TestC tC = new TestC();new Thread(tC).start();
    }
}

class TestA implements Runnable {
    @Override
    public void run() {
        try {
            Transaction tx = new Transaction();
            System.out.println("Tx A --> TxNum: "+tx.getTxNum());
            Block blk1 = new Block("junk", 1);
            Block blk2 = new Block("junk", 2);
            tx.pin(blk1);
            tx.pin(blk2);
            System.out.println("Tx A: read block 1 start");
            String blk_1_pos_20_val = tx.getString(blk1, 20);
            System.out.println("Tx A  read block 1 at pos 20: "+blk_1_pos_20_val);
            System.out.println("Tx A: read block 1 end");

            Thread.sleep(1000);

            System.out.println("Tx A: read block 2 start");
            int blk_2_pos_88_val = tx.getInt(blk2, 88);
            System.out.println("Tx A  read block 2 at pos 88: "+blk_2_pos_88_val);
            System.out.println("Tx A: read block 2 end");

            tx.commit();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class TestB implements Runnable {
    @Override
    public void run() {
        try {
            Transaction tx = new Transaction();
            System.out.println("Tx B --> TxNum: "+tx.getTxNum());
            Block blk1 = new Block("junk", 1);
            Block blk2 = new Block("junk", 2);
            tx.pin(blk1);
            tx.pin(blk2);

            System.out.println("Tx B: write block 2 start");
            tx.setInt(blk2, 88, 2);
            System.out.println("Tx B write block 2 at pos 88 with value \'2\' success!");
            System.out.println("Tx B: write block 2 end");

            Thread.sleep(1000);

            System.out.println("Tx B: read block 1 start");
            String blk_1_pos_20_val = tx.getString(blk1, 20);
            System.out.println("Tx B  read block 1 at pos 20: "+blk_1_pos_20_val);
            System.out.println("Tx B: read block 1 end");

            tx.commit();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class TestC implements Runnable {
    @Override
    public void run() {
        try {
            Transaction tx = new Transaction();
            System.out.println("Tx C --> TxNum: "+tx.getTxNum());
            Block blk1 = new Block("junk", 1);
            Block blk2 = new Block("junk", 2);
            tx.pin(blk1);
            tx.pin(blk2);

            System.out.println("Tx C: write block 1 start");
            tx.setString(blk1, 20, "hello");
            System.out.println("Tx C write block 1 at pos 20 with value \'hello\' success!");
            System.out.println("Tx C: write block 1 end");

            Thread.sleep(1000);

            System.out.println("Tx C: read block 2 start");
            int blk_2_pos_88_val = tx.getInt(blk2, 88);
            System.out.println("Tx C  read block 2 at pos 88: "+blk_2_pos_88_val);
            System.out.println("Tx C: read block 2 end");

            tx.commit();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}