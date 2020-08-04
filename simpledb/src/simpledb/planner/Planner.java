package simpledb.planner;

import simpledb.parser.*;
import simpledb.query.Plan;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @ClassName Planner
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-26 15:48
 * @Version 1.0
 **/

public class Planner {
    private QueryPlanner queryPlanner;
    private UpdatePlanner updatePlanner;

    public Planner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    public Plan createQueryPlan(String query, Transaction tx) throws IOException {
        Parser parser=new Parser(new Lexer(query));
        QueryData queryData=parser.query();
        //================ TODO ==================
        // 验证SQL语句语义正确性的代码
        // 见 19.2 小节
        //================ TODO ==================
        return queryPlanner.createPlan(queryData,tx);
    }
    public int executeUpdate(String cmd, Transaction tx) throws IOException {
        Parser parser=new Parser(new Lexer(cmd));
        Object obj=parser.updateCmd();
        //================ TODO ==================
        // 验证update SQL语句语义正确性的代码
        // 见 19.2 小节
        //================ TODO ==================
        if(obj instanceof InsertData)
            return  updatePlanner.executeInsert((InsertData) obj,tx);
        else if(obj instanceof DeleteData)
            return updatePlanner.executeDelete((DeleteData)obj,tx);
        else if(obj instanceof ModifyData)
            return updatePlanner.executeModify((ModifyData)obj,tx);
        else if(obj instanceof CreateTableData)
            return updatePlanner.executeCreateTable((CreateTableData)obj,tx);
        else if(obj instanceof CreateViewData)
            return updatePlanner.executeCreateView((CreateViewData)obj,tx);
        else if(obj instanceof CreateIndexData)
            return updatePlanner.executeCreateIndex((CreateIndexData)obj,tx);
        else
            return 0;
    }
}