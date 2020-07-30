package simpledb.metadata;

import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;

public class TableTest {
    public static void main(String[] args) throws IOException {
        SimpleDB.init("lzadb");
        MetadataMgr mtdMgr = SimpleDB.metadataMgr();

        Transaction tx = new Transaction();
        Schema schema = new Schema();
        schema.addStringField("DName", 10);
        schema.addIntField("DId");
        mtdMgr.createTable("dept", schema, tx);
        tx.commit();

        Transaction tx2 = new Transaction();
        TableInfo tableInfo = mtdMgr.getTableInfo("dept", tx2);
        RecordFile recordFile = new RecordFile(tableInfo, tx2);
        recordFile.insert();
        recordFile.setString("DName","C.S.");
        recordFile.setInt("DId",12);

        // 开始遍历
        recordFile.beforeFirst();
        while (recordFile.next())
            System.out.println(recordFile.getInt("DId") + ", " + recordFile.getString("DName"));
        recordFile.close();
        tx2.commit();
    }
}
