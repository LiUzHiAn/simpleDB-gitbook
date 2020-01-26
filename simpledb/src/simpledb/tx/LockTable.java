package simpledb.tx;

import simpledb.file.Block;

/**
 * @ClassName LockTable
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-25 17:21
 * @Version 1.0
 **/

public class LockTable {
    public void sLock(Block blk);
    public void xLock(Block blk);
    public void unLock(Block blk);
}