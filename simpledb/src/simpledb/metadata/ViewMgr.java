package simpledb.metadata;

import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @ClassName ViewMgr
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/5 5:06 下午
 * @Version 1.0
 */
public class ViewMgr {
    // 视图定义的最长字符串为150字节
    private static final int VIEW_DEF_MAX_LEN = 150;

    TableMgr tableMgr;

    public ViewMgr(boolean isNew, TableMgr tableMgr, Transaction tx) throws IOException {
        this.tableMgr = tableMgr;

        // 如果是新创建的数据库，则会创建一个viewcat元数据表
        if (isNew) {
            Schema schema = new Schema();
            // 视图名、表名、字段名 的最大长度都是一样的，为20个字符
            schema.addStringField("viewname", TableMgr.TABLE_AND_FIELD_NAME_MAX_LEN);
            schema.addStringField("viewdef", VIEW_DEF_MAX_LEN);
            tableMgr.createTable("viewcat", schema, tx);
        }
    }

    /**
     * 将一个新视图的元数据插入viewcat表中
     *
     * @param viewName
     * @param viewDef
     * @param tx
     */
    public void createView(String viewName, String viewDef, Transaction tx) throws IOException {
        TableInfo viewCatTableInfo = this.tableMgr.getTableInfo("viewcat", tx);

        RecordFile viewCatRecordFile = new RecordFile(viewCatTableInfo, tx);
        viewCatRecordFile.insert(); // 找到一个插入的位置
        viewCatRecordFile.setString("viewname", viewName);
        viewCatRecordFile.setString("viewdef", viewDef);
        viewCatRecordFile.close();

    }

    /**
     * 检索某个视图的定义
     *
     * @param viewName 指定视图名
     * @param tx
     * @return
     */
    public String getViewDef(String viewName, Transaction tx) throws IOException {
        String result = null;
        TableInfo viewCatTableInfo = this.tableMgr.getTableInfo("viewcat", tx);
        RecordFile viewCatRecordFile = new RecordFile(viewCatTableInfo, tx);
        // 遍历各个视图元数据
        viewCatRecordFile.beforeFirst();
        while (viewCatRecordFile.next()) {
            if (viewCatRecordFile.getString("viewname").equals(viewName)) {
                result = viewCatRecordFile.getString("viewdef");
                break;
            }
        }
        viewCatRecordFile.close();
        return result;
    }
}
