package simpledb.parser;

import simpledb.query.Predicate;

import java.util.Collection;

/**
 * @ClassName QueryData
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-20 20:30
 * @Version 1.0
 **/

public class QueryData {

    private Collection<String> fields;
    private Collection<String> tables;
    private Predicate pred;

    public QueryData(Collection<String> fields,
                     Collection<String> tables,
                     Predicate pred) {
        this.fields = fields;
        this.tables = tables;
        this.pred = pred;
    }

    public Collection<String> getFields() {
        return fields;
    }

    public Collection<String> getTables() {
        return tables;
    }

    public Predicate getPred() {
        return pred;
    }

    /**
     * 重新构造形如 Select XXX from XXX where XXX的SQL字符串。
     *
     * @return
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("select ");
        for (String f : fields)
            result.append(f).append(", ");
        // 移除循环最后添加的一个逗号
        result.substring(0, result.length() - 2);

        result.append(" from ");

        for (String tblName : tables)
            result.append(tblName).append(", ");
        // 再次移除循环最后添加的一个逗号
        result.substring(0, result.length() - 2);

        String predStr = pred.toString();
        if (!predStr.equals(""))
            result.append(" where ").append(predStr);

        return result.toString();
    }
}