package simpledb.query;

import java.io.IOException;

/**
 * @ClassName ProductScan
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/12 4:35 下午
 * @Version 1.0
 */
public class ProductScan implements Scan {
    private Scan scan1,scan2;

    public ProductScan(Scan scan1, Scan scan2) throws IOException {
        this.scan1 = scan1;
        this.scan2 = scan2;
        scan1.next();
    }

    // ============Scan 接口中相关方法的实现==============

    @Override
    public void beforeFirst() throws IOException {
        scan1.beforeFirst();
        scan1.next();
        scan2.beforeFirst();
    }

    @Override
    public boolean next() throws IOException {
        if (scan2.next())
            return true;
        else {
            scan2.beforeFirst();
            return scan1.next() && scan2.next();
        }
    }

    @Override
    public void close() throws IOException {
        scan1.next();
        scan2.next();
    }

    @Override
    public Constant getVal(String fieldName) {
        if(scan1.hasField(fieldName))
            return scan1.getVal(fieldName);
        else
            return scan2.getVal(fieldName);
    }

    @Override
    public int getInt(String fieldName) {
        if(scan1.hasField(fieldName))
            return scan1.getInt(fieldName);
        else
            return scan2.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        if(scan1.hasField(fieldName))
            return scan1.getString(fieldName);
        else
            return scan2.getString(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return scan1.hasField(fieldName) || scan2.hasField(fieldName);
    }
}
