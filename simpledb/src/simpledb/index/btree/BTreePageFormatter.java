package simpledb.index.btree;

import simpledb.buffer.PageFormatter;
import simpledb.file.Page;
import simpledb.record.Schema;
import simpledb.record.TableInfo;

import static simpledb.file.Page.BLOCK_SIZE;
import static simpledb.file.Page.INT_SIZE;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/09/23 23:11
 */
public class BTreePageFormatter implements PageFormatter {

    private TableInfo tableInfo;
    private int flag; // B树页的标志位,-1表示B树的叶子结点

    public BTreePageFormatter(TableInfo tableInfo, int flag) {
        this.tableInfo = tableInfo;
        this.flag = flag;
    }

    /**
     * 格式化一个新的B树页。
     * <p>
     * 尽可能在页中申请全部为空的records，字符串字段取值全为"",字符串字段取值全为0
     * <p>
     * 页的前INT_SIZE个字节为标志位，页的INT_SIZE～INT_SIZE*2 个字节为页上的记录数量
     *
     * @param p
     */
    @Override
    public void format(Page p) {
        p.setInt(0, flag); // 标志位
        p.setInt(INT_SIZE, 0); // 记录数格式化为0
        int recordLen = tableInfo.recordLength();
        for (int pos = INT_SIZE * 2; pos + recordLen <= BLOCK_SIZE; pos += recordLen)
            initializeDefaultRecord(p, pos);
    }

    private void initializeDefaultRecord(Page p, int pos) {
        Schema schema = tableInfo.schema();
        for (String fieldName : schema.fields()) {
            int fldOffset = tableInfo.offset(fieldName);
            if (tableInfo.schema().type(fieldName) == Schema.INTEGER)
                p.setInt(pos + fldOffset, 0);
            else
                p.setString(pos + fldOffset, "");
        }
    }
}
