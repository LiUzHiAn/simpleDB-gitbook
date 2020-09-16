package simpledb.metadata;

import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static simpledb.metadata.TableMgr.TABLE_AND_FIELD_NAME_MAX_LEN;

/**
 * @ClassName IndexMgr
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/6 2:00 下午
 * @Version 1.0
 */
public class IndexMgr {

    private TableInfo tableInfo;

    /**
     * 创建IndexMgr对象。
     * <p>
     * IndexMgr会把索引的信息存储在表idxcat中，该表的schema如下：
     * (indexname, tablename, fieldname)
     * <p>
     * 分别代表索引名、被索引的表名、被索引的字段名
     *
     * @param isNew
     * @param tableMgr
     * @param tx
     */
    public IndexMgr(boolean isNew, TableMgr tableMgr, Transaction tx) throws IOException {

        if (isNew) {
            Schema schema = new Schema();
            schema.addStringField("indexname", TABLE_AND_FIELD_NAME_MAX_LEN);
            schema.addStringField("tablename", TABLE_AND_FIELD_NAME_MAX_LEN);
            schema.addStringField("fieldname", TABLE_AND_FIELD_NAME_MAX_LEN);
            tableMgr.createTable("idxcat", schema, tx);
        }
        tableInfo = tableMgr.getTableInfo("idxcat", tx);
    }

    /**
     * 创建一个索引
     *
     * @param idxName 索引名
     * @param tblName 被索引的表名
     * @param fldName 被索引的字段名
     * @param tx
     * @throws IOException
     */
    public void createIndex(String idxName, String tblName,
                            String fldName, Transaction tx) throws IOException {
        RecordFile idxCatRecordFile = new RecordFile(tableInfo, tx);
        idxCatRecordFile.insert(); // 找到一个插入位置
        idxCatRecordFile.setString("indexname", idxName);
        idxCatRecordFile.setString("tablename", tblName);
        idxCatRecordFile.setString("fieldname", fldName);
        idxCatRecordFile.close();
    }

    /**
     * 获取到指定表的所有索引信息
     *
     * @param tblName 被索引的表
     * @param tx
     * @return
     */
    public Map<String, IndexInfo> getIndexInfo(String tblName, Transaction tx) throws IOException {
        Map<String, IndexInfo> result = new HashMap<>();
        RecordFile idxCatRecordFile = new RecordFile(tableInfo, tx);
        idxCatRecordFile.beforeFirst();
        while (idxCatRecordFile.next()) {
            if (idxCatRecordFile.getString("tablename").equals(tblName)) {
                String idxName = idxCatRecordFile.getString("indexname");
                String fldName = idxCatRecordFile.getString("fieldname");

                IndexInfo indexInfo = new IndexInfo(idxName, tblName, fldName, tx);
                result.put(fldName, indexInfo);
            }
        }
        idxCatRecordFile.close();

        return result;
    }
}
