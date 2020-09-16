package simpledb.index;

import simpledb.query.Constant;
import simpledb.record.RID;

import java.io.IOException;

/**
 * @ClassName Index
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/9/6 7:36 下午
 * @Version 1.0
 */
public interface Index {
    public void beforeFirst(Constant searchKey) throws IOException;
    public boolean next() throws IOException;
    public RID getDataRid();
    public void insert(Constant dataval,RID datarid) throws IOException;
    public void delete(Constant dataval,RID datarid) throws IOException;
    public void close();
}
