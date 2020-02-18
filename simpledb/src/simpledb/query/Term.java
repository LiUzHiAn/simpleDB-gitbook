package simpledb.query;

import simpledb.record.Schema;

/**
 * @ClassName Trem
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/18 8:11 下午
 * @Version 1.0
 */
public class Term {
    private Expression lhs, rhs;

    public Term(Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    /**
     * 一个谓词项使得记录数减少的因子。
     * <p>
     * 详情参见图 17-12
     *
     * @param plan
     * @return
     */
    public int reductionFactor(Plan plan) {
        String lhsName, rhsName;
        // 1. 左右两边都是字段
        // 图17-12 中的 max{V(s_1,A), V(s_1,B)}
        if (lhs.isFieldName() && rhs.isFieldName()) {
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();
            return Math.max(plan.distinctValues(lhsName),
                    plan.distinctValues(rhsName));
        }
        // 2. 左边是字段名，右边是常量
        if (lhs.isFieldName()) {
            // 图17-12 中的 V(s_1,A)
            lhsName = lhs.asFieldName();
            return plan.distinctValues(lhsName);
        }
        // 3. 左边是常量，右边是字段名
        if (rhs.isFieldName()) {
            // 图17-12 中的 V(s_1,A)
            rhsName = rhs.asFieldName();
            return plan.distinctValues(rhsName);
        }
        // 4. 左右两边全是常量
        if (lhs.asConstant().equals(rhs.asConstant()))
            return 1;
        else
            return Integer.MAX_VALUE;
    }

    public Constant equatesWithConstant(String fldName) {
        // 左边是字段名，右边是常量
        if (lhs.isFieldName() && lhs.asFieldName().equals(fldName) && rhs.isConstant())
            return rhs.asConstant();
            // 左边是常量，右边是字段名
        else if (rhs.isFieldName() && rhs.asFieldName().equals(fldName) && lhs.isConstant())
            return lhs.asConstant();
        else
            return null;
    }

    /**
     * 判断当前项是否是形如"A=B"的项，
     * <p>
     * 其中A是指定的字段名， B是另一个字段名
     *
     * @param fldName 指定的字段名
     * @return 另一个字段名
     */
    public String equatesWithField(String fldName) {
        // 指定的字段在左边
        if (lhs.isFieldName() &&
                lhs.asFieldName().equals(fldName) &&
                rhs.isFieldName()) {
            return rhs.asFieldName();
            // 指定的字段在左边
        } else if (rhs.isFieldName() &&
                rhs.asFieldName().equals(fldName) &&
                lhs.isFieldName()) {
            return lhs.asFieldName();
        } else
            return null;
    }

    public boolean appliesTo(Schema schema) {
        return lhs.appliesTo(schema) && rhs.appliesTo(schema);
    }

    /**
     * 判断一个项左右两边是否相等
     *
     * @param scan
     * @return
     */
    public boolean isSatisfied(Scan scan) {
        Constant lhsVal = lhs.evaluate(scan);
        Constant rhsVal = rhs.evaluate(scan);
        return lhsVal.equals(rhsVal);
    }

    public String toString() {
        return lhs.toString() + " = " + rhs.toString();
    }
}
