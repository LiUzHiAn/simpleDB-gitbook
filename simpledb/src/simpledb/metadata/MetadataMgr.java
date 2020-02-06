package simpledb.metadata;

import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.Map;

/**
 * @ClassName MetadataMgr
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/4 10:39 下午
 * @Version 1.0
 */

public class MetadataMgr {
    private static TableMgr tableMgr;
    private static ViewMgr viewMgr;
    private static StatMgr statMgr;
    private static IndexMgr indexMgr;

    public MetadataMgr(boolean isNew, Transaction tx) throws IOException {
        tableMgr = new TableMgr(isNew, tx);
        viewMgr = new ViewMgr(isNew, tableMgr, tx);
        statMgr = new StatMgr(tableMgr, tx);
        indexMgr = new IndexMgr(isNew, tableMgr, tx);

    }

    public void createTable(String tblName, Schema schema, Transaction tx) throws IOException {
        tableMgr.createTable(tblName, schema, tx);
    }

    public TableInfo getTableInfo(String tblName, Transaction tx) throws IOException {
        return tableMgr.getTableInfo(tblName, tx);
    }

    public void createView(String viewName, String viewDef, Transaction tx) throws IOException {
        viewMgr.createView(viewName, viewDef, tx);
    }

    public String getViewDef(String viewName, Transaction tx) throws IOException {
        return viewMgr.getViewDef(viewName, tx);
    }

    public void createIndex(String indexName, String tblName,
                            String fieldName, Transaction tx) throws IOException {
        indexMgr.createIndex(indexName,tblName,fieldName,tx);
    }

    public Map<String, IndexInfo> getIndexInfo(String tblName, Transaction tx) throws IOException {
        return indexMgr.getIndexInfo(tblName,tx);
    }

    public StatInfo getStatInfo(String tblName, Transaction tx) throws IOException {
        return statMgr.getStatInfo(tblName,tx);
    }

}
