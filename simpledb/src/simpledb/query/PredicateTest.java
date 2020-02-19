package simpledb.query;

/**
 * @ClassName PredicateTest
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/18 11:07 下午
 * @Version 1.0
 */
public class PredicateTest {
    public static void main(String[] args) {
        Expression lhs1=new FieldNameExpression("SName");
        Expression rhs1=new ConstantExpression(new StringConstant("joe"));
        Term term1=new Term(lhs1,rhs1);

        Expression lhs2=new FieldNameExpression("MajorId");
        Expression rhs2=new FieldNameExpression("DId");
        Term term2=new Term(lhs2,rhs2);

        Predicate pred1=new Predicate(term1);
        System.out.println(pred1);
        pred1.conjoinWith(new Predicate(term2));
        System.out.println(pred1);
    }
}
