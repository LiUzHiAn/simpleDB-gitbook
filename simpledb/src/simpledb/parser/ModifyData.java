package simpledb.parser;

import simpledb.query.Expression;
import simpledb.query.Predicate;

/**
 * @ClassName ModifyData
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-20 21:10
 * @Version 1.0
 **/

public class ModifyData {
    private String tblName;
    private String fldName;
    private Expression newVal;
    private Predicate pred;

    public ModifyData(String tblName, String fldName,
                      Expression newVal, Predicate pred) {
        this.tblName = tblName;
        this.fldName = fldName;
        this.newVal = newVal;
        this.pred = pred;
    }

    public String getTblName() {
        return tblName;
    }

    public String getFldName() {
        return fldName;
    }

    public Expression getNewVal() {
        return newVal;
    }

    public Predicate getPred() {
        return pred;
    }
}