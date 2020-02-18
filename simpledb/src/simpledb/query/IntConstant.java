package simpledb.query;

/**
 * @ClassName IntConstant
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/18 7:34 下午
 * @Version 1.0
 */
public class IntConstant implements Constant {
    private Integer val;

    public IntConstant(Integer val) {
        this.val = val;
    }

    @Override
    public int hashCode() {
        return val.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        IntConstant ic = (IntConstant) obj;
        return ic != null && val.equals(ic.val);
    }


    @Override
    public String toString() {
        return val.toString();
    }


    @Override
    public Object asJavaVal() {
        return val;
    }

    @Override
    public int compareTo(Constant o) {
        IntConstant ic = (IntConstant) o;
        return val.compareTo(ic.val);
    }
}
