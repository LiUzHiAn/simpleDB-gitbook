package simpledb.planner;

import simpledb.parser.QueryData;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName BasicQueryPlanner
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-26 17:24
 * @Version 1.0
 **/

public class BasicQueryPlanner implements QueryPlanner {

    @Override
    public Plan createPlan(QueryData queryData, Transaction tx) throws IOException {
        // 步骤1: 对每个表或视图，创建一个plan
        List<Plan> plans = new ArrayList<>();
        for (String tblName : queryData.getTables()) {
            String viewDef = SimpleDB.metadataMgr().getViewDef(tblName, tx);
            if (null != viewDef)
                plans.add(SimpleDB.planner().createQueryPlan(viewDef, tx));
            else
                plans.add(new TablePlan(tblName, tx));
        }

        // 步骤2: 依次做product操作
        Plan p = plans.remove(0);
        for (Plan nextPlan : plans) {
            p = new ProductPlan(p, nextPlan);
        }

        // 步骤3: 根据where部分的谓词筛选
        p=new SelectPlan(p,queryData.getPred());

        // 步骤4: 根据select部分做project操作
        p=new ProjectPlan(p,queryData.getFields());

        return p;
    }
}