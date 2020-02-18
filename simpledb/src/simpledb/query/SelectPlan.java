package simpledb.query;

import simpledb.record.Schema;

import java.io.IOException;

/**
 * @ClassName SelectPlan
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/13 6:59 下午
 * @Version 1.0
 */
public class SelectPlan implements Plan {
    private Plan plan;
    private Predicate predicate;

    public SelectPlan(Plan plan, Predicate predicate) {
        this.plan = plan;
        this.predicate = predicate;
    }

    @Override
    public Scan open() throws IOException {
        Scan scan = plan.open();
        return new SelectScan(scan, predicate);
    }

    @Override
    public int blockAccessed() {
        return plan.blockAccessed();
    }

    @Override
    public int recordsOutput() {
        // predicate.reductionFactor(plan)就是 图17-13 中的 V(s_1, A)
        return plan.recordsOutput() / predicate.reductionFactor(plan);
    }

    @Override
    public int distinctValues(String fldName) {
        if (null != predicate.equatesWithConstant(fldName))
            return 1;
        else {
            String theOtherFldName = predicate.equatesWithField(fldName);
            if (theOtherFldName != null)
                return Math.min(plan.distinctValues(fldName),
                        plan.distinctValues(theOtherFldName));
            else
                return Math.min(recordsOutput(), plan.distinctValues(fldName));
        }

    }

    @Override
    public Schema schema() {
        return plan.schema();
    }
}
