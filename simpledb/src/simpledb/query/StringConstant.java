package simpledb.query;

/**
 * @ClassName StringConstan
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/18 7:40 下午
 * @Version 1.0
 */
public class StringConstant implements Constant {
    private String val;

    public StringConstant(String val) {
        this.val = val;
    }

    @Override
    public int hashCode() {
        return val.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        StringConstant sc = (StringConstant) obj;
        return sc != null && val.equals(sc.val);
    }


    @Override
    public String toString() {
        return val;
    }


    @Override
    public Object asJavaVal() {
        return val;
    }

    @Override
    public int compareTo(Constant o) {
        StringConstant sc = (StringConstant) o;
        return val.compareTo(sc.val);
    }
}

