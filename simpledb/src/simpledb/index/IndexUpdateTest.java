package simpledb.index;

import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.planner.Planner;
import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexUpdateTest {

    public static void main(String[] args) throws IOException {
        // suppose the STUDENT(sname varchar(5), majorid int) table already exists
        SimpleDB.init("studentDB");
        Transaction tx = new Transaction();

        // create a table STUDENT(sname, majorid) and a index on STUDENT(majorid)
        Planner planner = SimpleDB.planner();
        String str = "create table student (sname varchar(5), majorid int)";
        planner.executeUpdate(str, tx);
        String createIndexStr = "create index stuMajorIdIndex on student(majorid)";
        planner.executeUpdate(createIndexStr, tx);

        // open a scan to table
        Plan stuPlan = new TablePlan("student", tx);
        TableScan stuScan = (TableScan) stuPlan.open();

        // open the index on MajorId
        MetadataMgr mdMgr = SimpleDB.metadataMgr();
        Map<String, IndexInfo> indexInfos = mdMgr.getIndexInfo("student", tx);
        Map<String, Index> indexes = new HashMap<>();
        for (String fldName : indexInfos.keySet()) {
            Index idx = indexInfos.get(fldName).open();
            indexes.put(fldName, idx);
        }

        // Task 1: insert a two STUDENT record
        // insert the data record first
        stuScan.insert();
        stuScan.setString("sname", "Sam");
        stuScan.setInt("majorid", 10);
        // then insert a corresponding index record
        RID datarid = stuScan.getRID();
        for (String fldName : indexes.keySet()) {
            Constant dataval = stuScan.getVal(fldName);
            Index idx = indexes.get(fldName);
            idx.insert(dataval, datarid);
        }

        // insert the data record first
        stuScan.insert();
        stuScan.setString("sname", "Andy");
        stuScan.setInt("majorid", 10);
        // then insert a corresponding index record
        for (String fldName : indexes.keySet()) {
            Constant dataval = stuScan.getVal(fldName);
            Index idx = indexes.get(fldName);
            idx.insert(dataval, stuScan.getRID());
        }

        // Task 2: find and delete Sam's record
        stuScan.beforeFirst();
        while (stuScan.next()) {
            if (stuScan.getString("sname").equals("Sam")) {
                // delete the corresponding index record(s) first
                RID rid = stuScan.getRID();
                for (String idxFldName : indexes.keySet()) {
                    Constant dataval = stuScan.getVal(idxFldName);
                    Index idx = indexes.get(idxFldName);
                    idx.delete(dataval, rid);
                }

                // then delete the data record
                stuScan.delete();
                break; // todo why break here???
            }
        }

        // close the resources
        stuScan.close();
        for (Index idx : indexes.values()) {
            idx.close();
        }
        tx.commit();
    }
}
