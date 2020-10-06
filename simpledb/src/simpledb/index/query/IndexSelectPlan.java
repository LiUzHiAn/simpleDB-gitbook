package simpledb.index.query;

import simpledb.index.Index;
import simpledb.metadata.IndexInfo;
import simpledb.query.Constant;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TableScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/10/05 14:15
 */
public class IndexSelectPlan implements Plan {
    private Plan plan;
    private IndexInfo indexInfo;
    private Constant constantVal;

    public IndexSelectPlan(Plan plan, IndexInfo indexInfo, Constant constantVal) {
        this.plan = plan;
        this.indexInfo = indexInfo;
        this.constantVal = constantVal;
    }

    @Override
    public Scan open() throws IOException {
        TableScan tableScan= (TableScan) plan.open();
        Index idx=indexInfo.open();  // 打开索引
        return new IndexSelectScan(tableScan,idx,constantVal);
    }

    @Override
    public int blockAccessed() {
        return indexInfo.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return indexInfo.recordsOutput();
    }

    @Override
    public int distinctValues(String fldName) {
        return indexInfo.distinctValues(fldName);
    }

    @Override
    public Schema schema() {
        return plan.schema();
    }
}

