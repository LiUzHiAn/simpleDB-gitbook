package simpledb.query;

import simpledb.record.Schema;

import java.io.IOException;
import java.util.Collection;

/**
 * @ClassName ProjectPlan
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/13 7:16 下午
 * @Version 1.0
 */
public class ProjectPlan implements Plan {
    private Plan plan;
    private Schema schema = new Schema();

    public ProjectPlan(Plan plan, Collection<String> fieldList) {
        this.plan=plan;
        for (String fieldName : fieldList)
            schema.add(fieldName,plan.schema());
    }

    @Override
    public Scan open() throws IOException {
        Scan scan=plan.open();
        return  new ProjectScan(scan,schema.fields());
    }

    @Override
    public int blockAccessed() {
        return plan.blockAccessed();
    }

    @Override
    public int recordsOutput() {
        return plan.recordsOutput();
    }

    @Override
    public int distinctValues(String fldName) {
        return plan.distinctValues(fldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
