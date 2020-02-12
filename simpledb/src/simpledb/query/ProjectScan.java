package simpledb.query;

import java.io.IOException;
import java.util.Collection;

/**
 * @ClassName ProjectScan
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/12 4:28 下午
 * @Version 1.0
 */
public class ProjectScan implements Scan {
    private Scan scan;
    private Collection<String> fieldList;

    public ProjectScan(Scan scan, Collection<String> fieldList) {
        this.scan = scan;
        this.fieldList = fieldList;
    }

    // ============Scan 接口中相关方法的实现==============

    @Override
    public void beforeFirst() throws IOException {
        scan.beforeFirst();
    }

    @Override
    public boolean next() throws IOException {
        return scan.next();
    }

    @Override
    public void close() throws IOException {
        scan.close();
    }

    @Override
    public Constant getVal(String fieldName) {
        if (hasField(fieldName))
            return scan.getVal(fieldName);
        else
            throw new RuntimeException("Field "+fieldName+" is not found!");
    }

    @Override
    public int getInt(String fieldName) {
        if (hasField(fieldName))
            return scan.getInt(fieldName);
        else
            throw new RuntimeException("Field "+fieldName+" is not found!");
    }

    @Override
    public String getString(String fieldName) {
        if (hasField(fieldName))
            return scan.getString(fieldName);
        else
            throw new RuntimeException("Field "+fieldName+" is not found!");
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldList.contains(fieldName);
    }
}
