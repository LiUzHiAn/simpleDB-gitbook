package simpledb.metadata;

import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName TableMgr
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/5 12:08 上午
 * @Version 1.0
 */
public class TableMgr {
    // 表名和字段名最大长度
    public static final int TABLE_AND_FIELD_NAME_MAX_LEN = 20;
    // tblcat表和fldcat表
    private TableInfo tblCatInfo, fldCatInfo;

    public TableMgr(boolean isNew, Transaction tx) throws IOException {
        // tblcat表
        Schema tblCatSchema = new Schema();
        tblCatSchema.addStringField("tblname", TABLE_AND_FIELD_NAME_MAX_LEN);
        tblCatSchema.addIntField("reclength");
        tblCatInfo = new TableInfo("tblcat", tblCatSchema);  // 存放在tblcat.tbl文件中

        // fldcat表
        Schema fldCatSchema = new Schema();
        fldCatSchema.addStringField("tblname", TABLE_AND_FIELD_NAME_MAX_LEN);
        fldCatSchema.addStringField("fldname", TABLE_AND_FIELD_NAME_MAX_LEN);
        fldCatSchema.addIntField("type"); // 字段类型
        fldCatSchema.addIntField("length"); // 对于非字符串字段没什么意义
        fldCatSchema.addIntField("offset"); // 各字段偏移量
        fldCatInfo = new TableInfo("fldcat", fldCatSchema);  // 存放在fldcat.tbl文件中

        // catalog表tblcat和fldcat自身的schema信息也被存入元数据表中
        if (isNew) {
            createTable("tblcat", tblCatSchema, tx);
            createTable("fldcat", fldCatSchema, tx);
        }
    }

    /**
     * 将一个新表的元数据到相应catalog表中。
     * <p>
     * 会在元数据表tblcat和fldcat中插入相应的记录。
     *
     * @param tblName
     * @param tblSchema
     * @param tx
     * @throws IOException
     */
    public void createTable(String tblName, Schema tblSchema, Transaction tx) throws IOException {
        TableInfo tableInfo = new TableInfo(tblName, tblSchema);

        // 插入一条记录到tblcat表中
        RecordFile tblCatRecordFile = new RecordFile(this.tblCatInfo, tx);
        tblCatRecordFile.insert(); // 找到一个插入位置
        tblCatRecordFile.setString("tblname", tblName);
        tblCatRecordFile.setInt("reclength", tableInfo.recordLength());
        tblCatRecordFile.close();

        // 为新建表的每个字段相应地插入一条记录到fldcat表中
        RecordFile fldCatRecordFile = new RecordFile(this.fldCatInfo, tx);
        for (String fieldName : tblSchema.fields()) {
            fldCatRecordFile.insert(); // 找到一个插入位置
            fldCatRecordFile.setString("tblname", tblName);
            fldCatRecordFile.setString("fldname", fieldName);
            fldCatRecordFile.setInt("type", tblSchema.type(fieldName));
            fldCatRecordFile.setInt("length", tblSchema.length(fieldName));
            fldCatRecordFile.setInt("offset", tableInfo.offset(fieldName));
        }
        fldCatRecordFile.close();
    }

    /**
     * 从元数据表中，构造一个已创建表的TableInfo对象。
     *
     * @param tblName
     * @param tx
     * @return
     * @throws IOException
     */
    public TableInfo getTableInfo(String tblName, Transaction tx) throws IOException {
        // 1. 先得到表粒度的信息
        RecordFile tblCatRecordFile = new RecordFile(this.tblCatInfo, tx);
        int recordLen = -1;
        // 从最前面开始搜索
        // TODO 新建一个RecordFile对象时会自动移到第一块，所以以下语句可以省略
        // tblCatRecordFile.beforeFirst();
        // TODO
        while (tblCatRecordFile.next()) {
            if (tblCatRecordFile.getString("tblname").equals(tblName)) {
                recordLen = tblCatRecordFile.getInt("reclength");
                break;
            }
        }
        tblCatRecordFile.close();

        // 2. 再得到字段粒度的信息
        RecordFile fldCatRecordFile = new RecordFile(this.fldCatInfo, tx);
        Schema schema = new Schema();
        Map<String, Integer> offsets = new HashMap<>();
        // 从最前面开始搜索
        // TODO 新建一个RecordFile对象时会自动移到第一块，所以以下语句可以省略
        fldCatRecordFile.beforeFirst();
        // TODO
        while (fldCatRecordFile.next()) {
            if (fldCatRecordFile.getString("tblname").equals(tblName)) {
                String fieldName = fldCatRecordFile.getString("fldname");
                int filedType = fldCatRecordFile.getInt("type");
                int filedLength = fldCatRecordFile.getInt("length");
                int filedOffset = fldCatRecordFile.getInt("offset");

                // 将各字段的偏移量，准备添加到TableInfo对象中去
                offsets.put(fieldName, filedOffset);
                // 将各字段，添加到schema对象中去
                schema.addField(fieldName, filedType, filedLength);
            }
        }
        fldCatRecordFile.close();

        return new TableInfo(tblName, schema, offsets, recordLen);
    }
}
