package simpledb.planner;

import simpledb.parser.*;
import simpledb.query.*;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ClassName BasicUpdatePlanner
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-26 18:12
 * @Version 1.0
 **/

public class BasicUpdatePlanner implements UpdatePlanner {
    @Override
    public int executeInsert(InsertData insertData, Transaction tx) throws IOException {
        Plan p = new TablePlan(insertData.getTblName(), tx);
        UpdateScan scan = (UpdateScan) p.open();
        scan.insert();
        // 插入的新记录各字段的取值
        Iterator<Constant> iter = insertData.getVals().iterator();
        for (String fieldName : insertData.getFields()) {
            Constant val = iter.next();
            scan.setVal(fieldName, val);
        }
        scan.close();
        return 1;
    }

    @Override
    public int executeDelete(DeleteData deleteData, Transaction tx) throws IOException {
        Plan p = new TablePlan(deleteData.getTblName(), tx);
        p = new SelectPlan(p, deleteData.getPred());
        UpdateScan scan = (UpdateScan) p.open();
        int cnt = 0;
        while (scan.next()) {
            scan.delete();
            cnt++;
        }
        scan.close();
        return cnt;

    }

    @Override
    public int executeModify(ModifyData modifyData, Transaction tx) throws IOException {
        Plan p = new TablePlan(modifyData.getTblName(), tx);
        p = new SelectPlan(p, modifyData.getPred());
        UpdateScan scan = (UpdateScan) p.open();
        int cnt = 0;
        while (scan.next()) {
            scan.setVal(modifyData.getFldName(),
                    modifyData.getNewVal().asConstant());
            cnt++;
        }
        scan.close();
        return cnt;
    }

    @Override
    public int executeCreateTable(CreateTableData data, Transaction tx) throws IOException {
        SimpleDB.metadataMgr().createTable(data.getTblName(),
                data.getSchema(),
                tx);
        return 0;
    }

    @Override
    public int executeCreateView(CreateViewData data, Transaction tx) throws IOException {
        SimpleDB.metadataMgr().createView(data.getViewName(),
                data.getViewDef(),
                tx);
        return 0;
    }

    @Override
    public int executeCreateIndex(CreateIndexData data, Transaction tx) throws IOException {
        SimpleDB.metadataMgr().createIndex(data.getIndexName(),
                data.getTblName(),
                data.getFldName(),
                tx);
        return 0;
    }
}