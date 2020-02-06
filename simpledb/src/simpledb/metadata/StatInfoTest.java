package simpledb.metadata;

import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * @ClassName StatInfoTest
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/5 9:01 下午
 * @Version 1.0
 */
public class StatInfoTest {
    public static void main(String[] args) {
        SimpleDB.init("liuzhian/studentdb");
        MetadataMgr metadataMgr = SimpleDB.metadataMgr();

        Transaction tx = new Transaction();
        TableInfo tableInfo=metadataMgr.getTableInfo("student",tx);
        StatInfo statInfo=metadataMgr.getStatInfo("student",tableInfo,tx);

        System.out.println(statInfo.blocksAccessed() + " " +
                statInfo.recordsOutput() + " " +
                statInfo.distinctValues("majorid"));
        tx.commit();

    }
}
