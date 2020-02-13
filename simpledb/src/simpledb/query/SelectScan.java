package simpledb.query;

import simpledb.record.RID;

import java.io.IOException;

/**
 * @ClassName SelectScan
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/12 3:56 下午
 * @Version 1.0
 */
public class SelectScan implements UpdateScan {

    private Scan scan;
    private Predicate predicate;

    public SelectScan(Scan scan, Predicate predicate) {
        this.scan = scan;
        this.predicate = predicate;
    }

    //================Scan 接口中的方法实现===================

    @Override
    public void beforeFirst() throws IOException {
        scan.beforeFirst();
    }

    @Override
    public boolean next() throws IOException {
        // 这里必须用while，而不是if
        while (scan.next())
            if (predicate.isSatisified(scan))
                return true;
        return false;
    }

    @Override
    public void close() throws IOException {
        scan.close();
    }

    @Override
    public Constant getVal(String fieldName) {
        return scan.getVal(fieldName);
    }

    @Override
    public int getInt(String fieldName) {
        return scan.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return scan.getString(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return scan.hasField(fieldName);
    }

    //================UpdateScan 接口中额外的方法实现===================

    @Override
    public void setVal(String fieldName, Constant newVal) {
        // 这里必须把scan强转为UpdateScan类型
        // 也只有UpdateScan类型的对象才有setXXX()方法
        // 如果scan不是一个实现了UpdateScan接口的对象，运行时则会抛出ClassCastException异常
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.setVal(fieldName,newVal);
    }

    @Override
    public void setInt(String fieldName, int newVal) {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.setInt(fieldName, newVal);
    }

    @Override
    public void setString(String fieldName, String newVal) {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.setString(fieldName, newVal);
    }

    @Override
    public void insert() throws IOException {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.insert();
    }

    @Override
    public void delete() {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.delete();
    }

    @Override
    public RID getRID() {
        UpdateScan updateScan = (UpdateScan) scan;
        return updateScan.getRID();
    }

    @Override
    public void moveToRId(RID rid) {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.moveToRId(rid);
    }
}
