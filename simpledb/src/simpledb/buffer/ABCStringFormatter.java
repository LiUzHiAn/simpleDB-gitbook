package simpledb.buffer;

import simpledb.file.Page;

import static simpledb.file.Page.BLOCK_SIZE;
import static simpledb.file.Page.STR_SIZE;

/**
 * @ClassName ABCStringFormatter
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-17 20:15
 * @Version 1.0
 **/

public class ABCStringFormatter implements PageFormatter {
    @Override
    public void format(Page p) {
        int recSize = STR_SIZE("abc".length());
        for (int i = 0; i + recSize <= BLOCK_SIZE; i += recSize) {
            p.setString(i, "abc");
        }
    }
}