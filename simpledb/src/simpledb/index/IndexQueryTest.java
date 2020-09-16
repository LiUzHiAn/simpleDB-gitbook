package simpledb.index;

import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.planner.Planner;
import simpledb.query.IntConstant;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.Map;

public class IndexQueryTest {

    public static void main(String[] args) throws IOException {
        SimpleDB.init("studentDB");
        Transaction tx = new Transaction();

        // open a scan to table
        Plan stuPlan = new TablePlan("student", tx);
        TableScan stuScan = (TableScan) stuPlan.open();

        // open the index on majorid
        MetadataMgr mdMgr = SimpleDB.metadataMgr();
        Map<String, IndexInfo> indexes = mdMgr.getIndexInfo("student", tx);
        IndexInfo ii = indexes.get("majorid");
        Index idx = ii.open();

        // retrieve all index records which have a dataval of 10
        idx.beforeFirst(new IntConstant(10));
        while (idx.next()) {
            // use the datarid to go to the corresponding record in table STUDENT
            RID dataRid = idx.getDataRid();
            stuScan.moveToRId(dataRid);
            System.out.println(stuScan.getString("sname"));
        }

        // close the idx, the table scan, and the transaction
        idx.close();
        stuScan.close();
        tx.commit();
    }
}
