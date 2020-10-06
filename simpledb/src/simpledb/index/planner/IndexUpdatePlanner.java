package simpledb.index.planner;

import simpledb.index.Index;
import simpledb.metadata.IndexInfo;
import simpledb.parser.*;
import simpledb.planner.UpdatePlanner;
import simpledb.query.*;
import simpledb.record.RID;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/10/05 16:57
 */
public class IndexUpdatePlanner implements UpdatePlanner {
    @Override
    public int executeInsert(InsertData insertData, Transaction tx) throws IOException {
        Plan plan = new TablePlan(insertData.getTblName(), tx);
        UpdateScan scan = (UpdateScan) plan.open();
        scan.insert();
        // 先获得当前记录（即待插入记录）的rid
        RID rid = scan.getRID();

        // 先获取到待插入表的所有索引信息
        Map<String, IndexInfo> indexInfoMap =
                SimpleDB.metadataMgr().getIndexInfo(insertData.getTblName(), tx);
        Iterator<Constant> valsIter = insertData.getVals().iterator();
        for (String fieldName : insertData.getFields()) {
            // 插入的新记录各字段的取值
            Constant val = valsIter.next();
            scan.setVal(fieldName, val);

            // 如果存在该字段的索引，则也许作出相应的修改
            IndexInfo indexInfo = indexInfoMap.get(fieldName);
            if (indexInfo != null) {
                Index idx = indexInfo.open();
                idx.insert(val, rid);  // 在索引中插入一条新的索引记录，格式为<dataval,datarid>
                idx.close();
            }
        }
        scan.close();
        return 1;
    }

    @Override
    public int executeDelete(DeleteData deleteData, Transaction tx) throws IOException {
        Plan p = new TablePlan(deleteData.getTblName(), tx);
        p = new SelectPlan(p, deleteData.getPred());
        UpdateScan scan = (UpdateScan) p.open();

        // 先获取到待插入表的所有索引信息
        Map<String, IndexInfo> indexInfoMap =
                SimpleDB.metadataMgr().getIndexInfo(deleteData.getTblName(), tx);
        int cnt = 0;
        while (scan.next()) {
            // 先从该表的所有索引中，删除掉当前rid的索引记录
            RID rid = scan.getRID();
            for (String fieldName : indexInfoMap.keySet()) {
                // 得到当前记录的dataval
                Constant dataval = scan.getVal(fieldName);
                Index idx = indexInfoMap.get(fieldName).open();
                idx.delete(dataval, rid);  // 删除掉索引记录
                idx.close();
            }
            // 再删除掉当前数据记录
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

        // 先获取到待插入表的所有索引信息
        Map<String, IndexInfo> indexInfoMap =
                SimpleDB.metadataMgr().getIndexInfo(modifyData.getTblName(), tx);
        // 目前只支持单字段modify
        IndexInfo indexInfo = indexInfoMap.get(modifyData.getFldName());
        Index index = (indexInfo == null) ? null : indexInfo.open();

        int cnt = 0;
        while (scan.next()) {
            // 首先，更新数据记录
            Constant newVal = modifyData.getNewVal().evaluate(scan);
            Constant oldVal = scan.getVal(modifyData.getFldName());
            scan.setVal(modifyData.getFldName(), newVal);

            // 再更新对应的索引字段
            if (index != null) {
                RID rid = scan.getRID();
                index.delete(oldVal, rid);  // 先删除旧的索引记录，再插入新的索引记录
                index.insert(newVal, rid);
            }
            cnt++;
        }
        if (index != null)
            index.close();
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
