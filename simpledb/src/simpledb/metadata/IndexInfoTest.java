package simpledb.metadata;

import simpledb.index.Index;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.Map;

/**
 * @ClassName IndexInfoTest
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/6 12:59 下午
 * @Version 1.0
 */
public class IndexInfoTest {
    public static void main(String[] args) throws IOException {
        SimpleDB.init("liuzhian/studentdb");
        Transaction tx = new Transaction();
        MetadataMgr metadataMgr = SimpleDB.metadataMgr();
        Map<String,IndexInfo> indexes = metadataMgr.getIndexInfo("student", tx);

        // Part 1: Print the name and cost of each index on STUDENT
//        for (String fieldName : indexes.keySet()) {
//            IndexInfo indexInfo = indexes.get(fieldName);
//            System.out.println(fieldName + "\t" + indexInfo.blocksAccessed());
//        }
//
//        // Part 2: Open the index on MajorId
//        IndexInfo ii = indexes.get("majorid");
//        Index idx = ii.open();
    }
}
