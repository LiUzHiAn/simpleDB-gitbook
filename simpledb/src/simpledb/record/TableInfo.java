package simpledb.record;

import java.util.HashMap;
import java.util.Map;

import static simpledb.file.Page.*;
import static simpledb.record.Schema.*;

/**
 * @ClassName TableInfo
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/3 1:14 下午
 * @Version 1.0
 */
public class TableInfo {
    private Schema schema;
    private Map<String, Integer> offsets;
    private int recordLen;
    private String tblName;

    public TableInfo(String tblName, Schema schema) {
        this.tblName = tblName;
        this.schema = schema;
        offsets = new HashMap<>();
        int pos = 0;
        for (String fieldName : schema.fields()) {
            offsets.put(fieldName, pos);
            // 每个字段需要的字节数
            pos += lengthInBytes(fieldName);
        }
        recordLen = pos;
    }

    public TableInfo(String tblName, Schema schema,
                     Map<String, Integer> offsets, int recordLen) {
        this.tblName = tblName;
        this.schema = schema;
        this.offsets = offsets;
        this.recordLen = recordLen;
    }

    /**
     * 返回保存该类型记录的文件名
     *
     * @return 文件名（包含后缀）
     */
    public String fileName() {
        return tblName + ".tbl";
    }

    public Schema schema() {
        return schema;
    }

    public int offset(String fldname) {
        return offsets.get(fldname);
    }

    public int recordLength() {
        return recordLen;
    }

    private int lengthInBytes(String fldName) {
        int fldType = schema.type(fldName);
        if (fldType == INTEGER) {
            return INT_SIZE;
        }
        else
            return STR_SIZE(schema.length(fldName));
    }
}
