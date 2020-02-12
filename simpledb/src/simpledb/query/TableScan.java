package simpledb.query;

import simpledb.record.RID;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @ClassName Table
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/11 9:46 下午
 * @Version 1.0
 */
public class TableScan implements UpdateScan {
    private RecordFile recordFile;
    private Schema schema;

    public TableScan(TableInfo tableInfo, Transaction tx) throws IOException {
        recordFile = new RecordFile(tableInfo, tx);
        schema = tableInfo.schema();
    }
    //================Scan 接口中的方法实现===================
    @Override
    public void beforeFirst() {
        recordFile.beforeFirst();
    }

    @Override
    public boolean next() throws IOException {
       return recordFile.next();
    }

    @Override
    public void close() {
        recordFile.close();
    }

    @Override
    public Constant getVal(String fieldName) {
        if (schema.type(fieldName)==Schema.VARCHAR)
            return new StringConstant(recordFile.getString(fieldName));
        else
            return new IntConstant(recordFile.getInt(fieldName));
    }

    @Override
    public int getInt(String fieldName) {
        return recordFile.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return recordFile.getString(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return schema.hasFiled(fieldName);
    }

    //================UpdateScan 接口中额外的方法实现===================

    @Override
    public void setVal(String fieldName, Constant newVal) {
        if (schema.type(fieldName)==Schema.VARCHAR)
            recordFile.setString(fieldName,(String) newVal.asJavaVal());
        else
            recordFile.setInt(fieldName,(Integer)newVal.asJavaVal());

    }

    @Override
    public void setInt(String fieldName, int newVal) {
        recordFile.setInt(fieldName,newVal);
    }

    @Override
    public void setString(String fieldName, String newVal) {
        recordFile.setString(fieldName,newVal);
    }

    @Override
    public void insert() throws IOException {
        recordFile.insert();
    }

    @Override
    public void delete() {
        recordFile.delete();
    }

    @Override
    public RID getRID() {
        return recordFile.currentRID();
    }

    @Override
    public void moveToRId(RID rid) {
        recordFile.moveToRID(rid);
    }
}
