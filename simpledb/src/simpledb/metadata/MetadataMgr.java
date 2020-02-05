package simpledb.metadata;

import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.util.Map;

/**
 * @ClassName MetadataMgr
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/4 10:39 下午
 * @Version 1.0
 */

public class MetadataMgr {

    public void createTable(String tblName, Schema schema, Transaction tx);
    public TableInfo getTableInfo(String tblName,Transaction tx);

    public void createView(String viewName,String viewDef,Transaction tx);
    public String getViewDef(String viewName,Transaction tx);

    public void createIndex(String indexName,String tblName,
                            String fieldName,Transaction tx);
    public Map<String,IndexInfo> getIndexInfo(String tblName,Transaction tx);

    public StatInfo getStatInfo(String tblName,TableInfo tableInfo,Transaction tx);

}
