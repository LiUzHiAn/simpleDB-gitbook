package simpledb.parser;

/**
 * @ClassName CreateViewData
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-20 21:25
 * @Version 1.0
 **/

public class CreateViewData {
    private String viewName;
    private QueryData qd;

    public CreateViewData(String viewName, QueryData qd) {
        this.viewName = viewName;
        this.qd = qd;
    }

    public String getViewName() {
        return viewName;
    }

    public String getViewDef() {
        return qd.toString();
    }
}