package simpledb.record;

import simpledb.buffer.PageFormatter;
import simpledb.file.Page;

import static simpledb.file.Page.BLOCK_SIZE;
import static simpledb.file.Page.INT_SIZE;
import static simpledb.record.RecordPage.EMPTY;

/**
 * @ClassName PageFormatter
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/3 5:07 下午
 * @Version 1.0
 */
public class RecordFormatter implements PageFormatter {

    private TableInfo tableInfo;

    public RecordFormatter(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    @Override
    public void format(Page p) {
        int recordSize = tableInfo.recordLength() + INT_SIZE;
        for (int pos = 0; pos + recordSize <= BLOCK_SIZE; pos += recordSize) {
            // 1. 先设置标志位为空
            p.setInt(pos, EMPTY);
            // 2. 再添加默认的记录值
            makeDefaultRecord(p, pos);
        }
    }

    private void makeDefaultRecord(Page page, int position) {
        for (String fldName : tableInfo.schema().fields()) {
            // 每个字段在一条记录内的offset
            int offset = tableInfo.offset(fldName);
            // int的默认值为0
            if (tableInfo.schema().type(fldName) == Schema.INTEGER)
                page.setInt(position + INT_SIZE + offset, 0);
            // string的默认值为""，即空串
            else
                page.setString(position + INT_SIZE + offset, "");
        }
    }
}
