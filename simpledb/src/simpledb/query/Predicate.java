package simpledb.query;

import simpledb.record.Schema;

/**
 * @ClassName Predicate
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/13 7:00 下午
 * @Version 1.0
 */
public class Predicate {
    // 供 Parser调用
    public void conjoinWith(Predicate predicate);

    // 供 SelectScan中的next()方法调用
    public boolean isSatisified(Scan s);

    // 供SelectPlan中的recordsOutput()方法调用
    public  int reductionFactor(Plan p);

    // 供 query planner调用
    public Predicate selectPred(Schema schema);
    public Predicate joinPred(Schema schema1,Schema schema2);
    public Constant equatesWithConstant(String fieldName);
    public String equatesWithField(String fieldName);

}
