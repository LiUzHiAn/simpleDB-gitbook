package simpledb.query;

import simpledb.record.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName Predicate
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/13 7:00 下午
 * @Version 1.0
 */
public class Predicate {
    private List<Term> terms = new ArrayList<>();

    public Predicate() {
    }

    /**
     * Creates a predicate containing a single term.
     * @param term
     */
    public Predicate(Term term) {
        this.terms.add(term);
    }

    // 供 Parser调用
    public void conjoinWith(Predicate predicate) {
        terms.addAll(predicate.terms);
    }

    /**
     * 判断一个谓词是否成立。
     * <p>
     * 只有谓词中所有的项均为真，整个谓词才为真。
     * <p>
     * 该方法供SelectScan中的next()方法调用
     *
     * @param s
     * @return
     */
    public boolean isSatisified(Scan s) {
        for (Term t : terms) {
            if (!t.isSatisfied(s))
                return false;
        }
        return true;
    }

    /**
     * 判断一个谓词使得输出记录数减少的因子。
     * <p>
     * 详情计算方法参见图 17-12。
     * <p>
     * 该方法供SelectPlan中的recordsOutput()方法调用
     *
     * @param p
     * @return
     */
    public int reductionFactor(Plan p) {
        int factor = 1;
        for (Term t : terms) {
            factor *= t.reductionFactor(p);
        }
        return factor;
    }

    /**
     * 返回满足指定schema的子谓词。
     * <p>
     * 该方法供 query planner调用
     *
     * @param schema
     * @return
     */
    public Predicate selectPred(Schema schema) {
        Predicate newPredicate = new Predicate();
        for (Term t : terms) {
            if (t.appliesTo(schema))
                newPredicate.terms.add(t);
        }
        if (newPredicate.terms.size() == 0)
            return null;
        else
            return newPredicate;
    }

    /**
     * 返回满足两个schema并集的子谓词。
     *
     * @param schema1
     * @param schema2
     * @return
     */
    public Predicate joinPred(Schema schema1, Schema schema2) {
        Predicate newPredicate = new Predicate();
        Schema newSchema = new Schema();
        newSchema.addAll(schema1);
        newSchema.addAll(schema2);
        for (Term t : terms) {
            if (!t.appliesTo(schema1) &&
                    !t.appliesTo(schema2) &&
                    t.appliesTo(newSchema))
                newPredicate.terms.add(t);
        }
        if (newPredicate.terms.size() == 0)
            return null;
        else
            return newPredicate;
    }

    /**
     * 判断一个谓词中是否存在形如"A=c"的项，
     * 其中A是指定字段名，c是指定的常量。
     * <p>
     * 如果有，返回该常量c；否则返回null。
     *
     * @param fieldName
     * @return
     */
    public Constant equatesWithConstant(String fieldName) {
        for (Term t : terms) {
            Constant result = t.equatesWithConstant(fieldName);
            if (result != null)
                return result;
        }
        return null;
    }

    /**
     * 判断一个谓词中是否存在形如"A=B"的项，
     * 其中A是指定字段名，B是另一个字段名
     * <p>
     * 如果有，返回指定字段名；否则返回null。
     *
     * @param fieldName 指定字段名A
     * @return 另一个字段名B
     */
    public String equatesWithField(String fieldName) {
        for (Term t : terms) {
            String result = t.equatesWithField(fieldName);
            if (result != null)
                return result;
        }
        return null;
    }

    public String toString() {
        Iterator<Term> iter = terms.iterator();
        if (!iter.hasNext())
            return "";
        String result = iter.next().toString();
        while (iter.hasNext())
            result += " and " + iter.next().toString();
        return result;
    }

}
