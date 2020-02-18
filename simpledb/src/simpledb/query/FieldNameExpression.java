package simpledb.query;

import simpledb.record.Schema;

/**
 * @ClassName FieldNameExpression
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/18 8:04 下午
 * @Version 1.0
 */
public class FieldNameExpression implements Expression {
    private String fldName;

    public FieldNameExpression(String fldName) {
        this.fldName = fldName;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isFieldName() {
        return true;
    }

    @Override
    public Constant asConstant() {
        throw  new ClassCastException();
    }

    @Override
    public String asFieldName() {
        return fldName;
    }

    @Override
    public Constant evaluate(Scan scan) {
        return scan.getVal(fldName);
    }

    @Override
    public boolean appliesTo(Schema schema) {
        return schema.hasFiled(fldName);
    }

    public String toString()
    {
        return fldName;
    }

}
