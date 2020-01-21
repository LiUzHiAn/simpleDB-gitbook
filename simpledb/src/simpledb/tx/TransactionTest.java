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
        Transaction tx = new Transaction();
        Block blk = new Block("junk", 3);
        tx.pin(blk);
        int n = tx.getInt(blk, 392);
        String str = tx.getString(blk, 20);
        tx.unpin(blk);
        System.out.println("Values are " + n + " and " + str);

        tx.pin(blk);
        n = tx.getInt(blk, 392); // 这条语句必要吗？？？
        tx.setInt(blk, 392, n + 1);
        tx.unpin(blk);

        PageFormatter pfmt = new ABCStringFormatter();
        Block newBlk = tx.append("junk", pfmt);
        tx.pin(newBlk);
        String s = tx.getString(newBlk, 0);
        assert (s.equals("abc"));
        tx.unpin(newBlk);
        int newBlkNum = newBlk.number();
        System.out.println("The first string in block "
                + newBlkNum + " is" + newBlkNum);

        tx.commit();
    }
}