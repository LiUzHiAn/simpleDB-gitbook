package simpledb.query;

import simpledb.record.Schema;

import java.io.IOException;

/**
 * @ClassName Plan
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/13 5:52 下午
 * @Version 1.0
 */
public interface Plan {
    public  Scan open() throws IOException;
    public  int blockAccessed();
    public int recordsOutput();
    public int distinctValues(String fldName);
    public Schema schema();
}
