package simpledb.planner;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.tx.recovery.LogRecord;
import simpledb.tx.recovery.LogRecordIterator;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ClassName PlannerTest
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-26 16:00
 * @Version 1.0
 **/

public class PlannerTest {

    public static void main(String[] args) throws IOException {
        SimpleDB.init("lzadb");

        Planner planner = SimpleDB.planner();
        Transaction tx = new Transaction();

        // 新建一个表
        String str = "create table student (sid int, sname varchar(5), age int)";
        planner.executeUpdate(str, tx);

        // 查询插入的表的信息
        TableInfo tableInfo = SimpleDB.metadataMgr().getTableInfo("student", tx);
        for (String fieldName : tableInfo.schema().fields())
            System.out.println(fieldName);

        // 插入一条记录到新建的表中
        String insertSQL = "insert into student (sid,sname,age) values (88,'andy',22)";
        int numRowsAffected = planner.executeUpdate(insertSQL, tx);
        System.out.println("插入" + numRowsAffected + "条数据成功！");

        // 查询刚才插入的数据
        String querySQL = "select sid,sname,age from student";
        Plan queryPlan = planner.createQueryPlan(querySQL, tx);
        Scan scan = queryPlan.open();
        while (scan.next()) {
            System.out.println(scan.getInt("sid") + " " +
                    scan.getString("sname") + " " +
                    scan.getInt("age"));
        }
        scan.close();

        tx.commit();


        // 查看日志
        int logRecordNum = 0;
        Iterator<LogRecord> iter = new LogRecordIterator();
        while (iter.hasNext()) {
            LogRecord record = iter.next();
            logRecordNum++;
            System.out.println(record);

        }
        System.out.println(logRecordNum);


//        // 处理一个update query
//        String cmd = "delete from STUDENT where MajorId=30";  // 大小写不敏感
//        int numAffected = planner.executeUpdate(cmd, tx);
//        System.out.println(numAffected + " records was affected!");
    }
}