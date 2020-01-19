package simpledb.buffer;

import simpledb.file.Page;

/**
 * @ClassName PageFormatter
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-17 19:33
 * @Version 1.0
 **/

public interface PageFormatter {
    public void format(Page p);
}