package simpledb.query;

import simpledb.metadata.StatInfo;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @ClassName TablePlan
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/13 6:04 下午
 * @Version 1.0
 */
public class TablePlan implements Plan {
    private Transaction tx;
    private TableInfo tableInfo;
    private StatInfo statInfo;

    public TablePlan(String tblName,Transaction tx) throws IOException {
        this.tx = tx;
        this.tableInfo = SimpleDB.metadataMgr().getTableInfo(tblName,tx);
        this.statInfo = SimpleDB.metadataMgr().getStatInfo(tblName,tx);
    }

    @Override
    public Scan open() throws IOException {
        return new TableScan(tableInfo,tx);
    }

    @Override
    public int blockAccessed() {
        return statInfo.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return statInfo.recordsOutput();
    }

    @Override
    public int distinctValues(String fldName) {
        return statInfo.distinctValues(fldName);
    }

    @Override
    public Schema schema() {
        return tableInfo.schema();
    }
}
