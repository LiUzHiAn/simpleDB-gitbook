package simpledb.parser;

import simpledb.record.Schema;

/**
 * @ClassName CreateTableData
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-20 21:23
 * @Version 1.0
 **/

public class CreateTableData {
    private String tblName;
    private Schema schema;
    public CreateTableData(String tblName, Schema schema) {
        this.tblName=tblName;
        this.schema=schema;
    }

    public String getTblName() {
        return tblName;
    }

    public Schema getSchema() {
        return schema;
    }
}