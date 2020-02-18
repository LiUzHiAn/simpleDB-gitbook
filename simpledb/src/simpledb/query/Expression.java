package simpledb.query;

import simpledb.record.Schema;

/**
 * @ClassName Expression
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/18 7:43 下午
 * @Version 1.0
 */
public interface Expression {
    public boolean isConstant();
    public boolean isFieldName();
    public Constant asConstant();
    public String asFieldName();
    public Constant evaluate(Scan scan);
    public boolean appliesTo(Schema schema);
}
