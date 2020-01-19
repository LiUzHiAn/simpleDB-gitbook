package simpledb.log;

import simpledb.file.Page;

import static simpledb.file.Page.INT_SIZE;
import static simpledb.file.Page.STR_SIZE;

/**
 * @ClassName BasicLogRecord
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-15 19:54
 * @Version 1.0
 **/

public class BasicLogRecord {

    private Page logPage;
    private int pos;


    public BasicLogRecord(Page logPage, int pos) {
        this.logPage = logPage;
        this.pos = pos;
    }

    public int nextInt() {
        int intVal = logPage.getInt(pos);
        pos += INT_SIZE;
        return intVal;
    }

    public String nextString() {
        String stringVal = logPage.getString(pos);
        pos += STR_SIZE(stringVal.length());
        return stringVal;
    }
}