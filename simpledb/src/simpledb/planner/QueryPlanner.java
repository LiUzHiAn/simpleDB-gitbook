package simpledb.planner;

import simpledb.parser.QueryData;
import simpledb.query.Plan;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @ClassName QueryPlanner
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-26 17:26
 * @Version 1.0
 **/

public interface QueryPlanner {
    public Plan createPlan(QueryData queryData, Transaction tx) throws IOException;
}