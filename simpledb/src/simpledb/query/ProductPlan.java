package simpledb.query;

import simpledb.record.Schema;

import java.io.IOException;

/**
 * @ClassName ProductPlan
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/13 7:25 下午
 * @Version 1.0
 */
public class ProductPlan implements Plan {
    private Plan plan1, plan2;
    private Schema schema = new Schema();

    public ProductPlan(Plan plan1, Plan plan2) {
        this.plan1 = plan1;
        this.plan2 = plan2;
        this.schema.addAll(plan1.schema());
        this.schema.addAll(plan2.schema());
    }

    @Override
    public Scan open() throws IOException {
        Scan scan1 = plan1.open();
        Scan scan2 = plan2.open();
        return new ProductScan(scan1, scan2);
    }

    /**
     * 图17-13中的 B(s_1) + R(s_1) * B(s_2)
     *
     * @return
     */
    @Override
    public int blockAccessed() {
        return plan1.blockAccessed() +
                (plan1.recordsOutput() * plan2.blockAccessed());
    }

    /**
     * 图17-13中的 R(s_1) * R(s_2)
     * @return
     */
    @Override
    public int recordsOutput() {
        return plan1.recordsOutput() * plan2.recordsOutput();
    }

    @Override
    public int distinctValues(String fldName) {
        if(plan1.schema().hasFiled(fldName))
            return plan1.distinctValues(fldName);
        else
            return plan2.distinctValues(fldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
