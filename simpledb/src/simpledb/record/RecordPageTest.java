package simpledb.record;

import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @ClassName RecordTest
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/3 3:06 下午
 * @Version 1.0
 */
public class RecordPageTest {
    public static void main(String[] args) throws IOException {

        Schema sch = new Schema();
        sch.addIntField("cid");
        sch.addStringField("title", 20);
        sch.addIntField("deptid");
        TableInfo ti = new TableInfo("course", sch);

        SimpleDB.init("lzadb");
        Transaction tx = new Transaction();
        Block blk = new Block(ti.fileName(), 3);
        RecordPage rp = new RecordPage(blk, ti, tx);

        // Part 1
        boolean ok = rp.insert();
        if (ok) {
            rp.setInt("cid", 82);
            rp.setString("title", "OO design");
            rp.setInt("deptid", 20);
        }
        ok = rp.insert();
        if (ok) {
            rp.setInt("cid", 80);
            rp.setString("title", "VB programming");
            rp.setInt("deptid", 30);
        }

        // Part 2 移到上第一条记录的前面，准备遍历
        // 这里一定要移到-1而不是0，因为searchFor()方法会先将currentSlot++
        rp.moveToID(-1);
        while (rp.next()) {
            int dept = rp.getInt("deptid");
            if (dept == 30)
                rp.delete();
            else if (rp.getString("title").equals("OO design"))
                rp.setString("title", "Object-Oriented");
        }

        tx.commit();


        Transaction tx2 = new Transaction();

        RecordFormatter recordFormatter = new RecordFormatter(ti);
        Block blk2 = tx2.append(ti.fileName(), recordFormatter);
        RecordPage rp2 = new RecordPage(blk2, ti, tx2);
        rp2.insert();

        tx2.commit();



    }
}
