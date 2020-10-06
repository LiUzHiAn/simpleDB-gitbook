package simpledb.index.query;

import simpledb.index.Index;
import simpledb.metadata.IndexInfo;
import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.query.TableScan;
import simpledb.record.Schema;

import java.io.IOException;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/10/05 15:43
 */
public class IndexJoinPlan implements Plan {
    private Plan plan1;
    private Plan plan2;
    private IndexInfo indexInfo;
    private Schema schema;
    private String joinField;

    public IndexJoinPlan(Plan plan1, Plan plan2, IndexInfo indexInfo, Schema schema, String joinField) {
        this.plan1 = plan1;
        this.plan2 = plan2;
        this.indexInfo = indexInfo;
        this.schema = new Schema();
        schema.addAll(plan1.schema());
        schema.addAll(plan2.schema());
        this.joinField = joinField;
    }

    @Override
    public Scan open() throws IOException {
        Scan scan1 = plan1.open();
        // 如果table2不是一张物理存储的表，抛出异常
        TableScan tableScan2 = (TableScan) plan2.open();
        Index idx = indexInfo.open();
        return new IndexJoinScan(scan1, tableScan2, idx, joinField);
    }

    @Override
    public int blockAccessed() {
        // 遍历plan1的块访问次数
        return plan1.blockAccessed() +
                // 对应plan1中的每个块，都需要检索一次索引
                plan1.recordsOutput() * indexInfo.blocksAccessed() +
                recordsOutput() ;  // todo: why here?
    }

    @Override
    public int recordsOutput() {
        return plan1.recordsOutput() * indexInfo.recordsOutput();
    }

    @Override
    public int distinctValues(String fldName) {
        if(plan1.schema().hasFiled(fldName))
            return plan1.distinctValues(fldName);
        else
            return plan2.distinctValues(fldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
