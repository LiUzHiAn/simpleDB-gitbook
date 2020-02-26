package simpledb.planner;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @ClassName PlannerTest
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-26 16:00
 * @Version 1.0
 **/

public class PlannerTest {

    public static void main(String[] args) throws IOException {
        SimpleDB.init("studentdb");
        Planner planner = SimpleDB.planner();
        Transaction tx = new Transaction();

        // 处理一个query
        String query = "select sname,gradyear from student";
        Plan queryPlan = planner.createQueryPlan(query, tx);
        Scan scan = queryPlan.open();
        while (scan.next()) {
            System.out.println(scan.getString("sname") + " " +
                    scan.getString("gradyear"));
        }
        scan.close();

        // 处理一个update query
        String cmd = "delete from STUDENT where MajorId=30";  // 大小写不敏感
        int numAffected = planner.executeUpdate(cmd, tx);
        System.out.println(numAffected + " records was affected!");
    }
}