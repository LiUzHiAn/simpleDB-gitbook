package simpledb.record;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @ClassName RecordFileTest
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/3 9:19 下午
 * @Version 1.0
 */
public class RecordFileTest {
    public static void main(String[] args) throws IOException {
        SimpleDB.init("liuzhian/simpledb");
        Transaction tx = new Transaction();
        Schema schema = new Schema();
        schema.addIntField("A");
        TableInfo tableInfo = new TableInfo("junk", schema);

        RecordFile recordFile = new RecordFile(tableInfo, tx);
        for (int i = 0; i < 10000; i++) {
            recordFile.insert();
            int n = (int) Math.round(Math.random() * 200);
            recordFile.setInt("A", n);
        }

        int cnt = 0;
        recordFile.beforeFirst();
        while (recordFile.next()) {
            if (recordFile.getInt("A") < 100) {
                recordFile.delete();
                cnt++;
            }
        }
        System.out.println("删除的记录数：" + cnt);
        recordFile.close();
        tx.commit();
    }
}
