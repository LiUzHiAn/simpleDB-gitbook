package simpledb.query;

import simpledb.record.Schema;

/**
 * @ClassName ConstanExpression
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/18 8:00 下午
 * @Version 1.0
 */
public class ConstantExpression implements Expression {
    private Constant val;

    public ConstantExpression(Constant val) {
        this.val = val;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean isFieldName() {
        return false;
    }

    @Override
    public Constant asConstant() {
        return val;
    }

    @Override
    public String asFieldName() {
        throw new ClassCastException();
    }

    @Override
    public Constant evaluate(Scan scan) {
        return val;
    }

    @Override
    public boolean appliesTo(Schema schema) {
        return true;
    }

    public String toString()
    {
        return val.toString();
    }

}
