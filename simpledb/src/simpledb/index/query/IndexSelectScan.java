package simpledb.index.query;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.TableScan;
import simpledb.record.RID;
import sun.tools.jconsole.Tab;

import java.io.IOException;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/10/05 14:28
 */
public class IndexSelectScan implements Scan {
    private TableScan tableScan;
    private Index index;
    private Constant constantVal;

    public IndexSelectScan(TableScan tableScan, Index index, Constant constantVal) throws IOException {
        this.tableScan = tableScan;
        this.index = index;
        this.constantVal = constantVal;
        beforeFirst();
    }

    @Override
    public void beforeFirst() throws IOException {
        index.beforeFirst(constantVal);
    }

    @Override
    public boolean next() throws IOException {
        boolean exist = index.next();
        if (exist) {
            RID rid = index.getDataRid();
            tableScan.moveToRId(rid);  // 底层表文件直接定位到rid位置的记录
        }
        return exist;
    }

    @Override
    public void close() throws IOException {
        index.close();
        tableScan.close();
    }

    @Override
    public Constant getVal(String fieldName) {
        return tableScan.getVal(fieldName);
    }

    @Override
    public int getInt(String fieldName) {
        return tableScan.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return tableScan.getString(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return tableScan.hasField(fieldName);
    }
}
