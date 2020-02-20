package simpledb.parser;

import simpledb.query.Predicate;

/**
 * @ClassName DeleteData
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-20 21:07
 * @Version 1.0
 **/

public class DeleteData {
    private String tblName;
    private Predicate pred;

    public DeleteData(String tblName, Predicate pred) {
        this.tblName = tblName;
        this.pred = pred;
    }

    public String getTblName() {
        return tblName;
    }

    public Predicate getPred() {
        return pred;
    }
}