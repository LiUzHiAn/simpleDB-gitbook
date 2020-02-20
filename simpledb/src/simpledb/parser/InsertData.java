package simpledb.parser;

import simpledb.query.Constant;

import java.util.Collection;

/**
 * @ClassName InsertData
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-20 21:02
 * @Version 1.0
 **/

public class InsertData {

    private String tblName;
    private Collection<String> fields;
    private Collection<Constant> vals;

    public InsertData(String tblName,
                      Collection<String> fields,
                      Collection<Constant> vals) {
        this.tblName = tblName;
        this.fields = fields;
        this.vals = vals;
    }

    public String getTblName() {
        return tblName;
    }

    public Collection<String> getFields() {
        return fields;
    }

    public Collection<Constant> getVals() {
        return vals;
    }
}