package simpledb.index.query;

import com.sun.xml.internal.bind.v2.model.core.ID;
import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.TableScan;
import simpledb.record.RID;

import java.io.IOException;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/10/05 16:01
 */
public class IndexJoinScan implements Scan {
    private Scan scan1;
    private TableScan tableScan2;
    private Index index;
    private String joinedField;

    public IndexJoinScan(Scan scan1, TableScan tableScan2, Index index, String joinedField) throws IOException {
        this.scan1 = scan1;
        this.tableScan2 = tableScan2;
        this.index = index;
        this.joinedField = joinedField;
        beforeFirst();
    }

    @Override
    public void beforeFirst() throws IOException {
        scan1.beforeFirst();
        scan1.next();
        resetIndex();  // 重置table2的索引
    }

    @Override
    public boolean next() throws IOException {
        while (true) {
            if (index.next()) {
                RID rid = index.getDataRid();
                tableScan2.moveToRId(rid);
                return true;
            }
            // scan1中也没有下一条记录了
            if (!scan1.next())
                return false;
            // 如果scan1还有下一条记录，那么将那条记录的索引字段取值，去重置table2的索引
            resetIndex();
        }
    }

    @Override
    public void close() throws IOException {
        scan1.close();
        tableScan2.close();
        index.close();
    }

    @Override
    public Constant getVal(String fieldName) {
        if(tableScan2.hasField(fieldName))
            return tableScan2.getVal(fieldName);
        else
            return scan1.getVal(fieldName);
    }

    @Override
    public int getInt(String fieldName) {
        if(tableScan2.hasField(fieldName))
            return tableScan2.getInt(fieldName);
        else
            return scan1.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        if(tableScan2.hasField(fieldName))
            return tableScan2.getString(fieldName);
        else
            return scan1.getString(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return scan1.hasField(fieldName) || tableScan2.hasField(fieldName);
    }

    /**
     * 重置table2的索引。
     * <p>
     * 先获取索引字段在scan1上的当前取值，并用该取值作为索引的取值
     */
    private void resetIndex() throws IOException {
        Constant searchKey = scan1.getVal(joinedField);
        index.beforeFirst(searchKey);
    }
}
