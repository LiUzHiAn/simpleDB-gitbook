package simpledb.tx.concurrency;

import simpledb.file.Block;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName concurrency
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-26 23:42
 * @Version 1.0
 **/

public class ConcurrencyMgr {
    // 全局锁表，所有的事务共享锁表
    private static LockTable lockTbl = new LockTable();
    // 当前事务的持有锁情况
    private Map<Block, String> locks = new HashMap<>();


    /**
     * 为当前事务请求指定块的共享锁。
     * <p>
     * 如果当前事务未持有共享锁，才会去为此事务请求对应块的共享锁
     *
     * @param blk 指定的块
     */
    public void sLock(Block blk) {
        if (null == locks.get(blk)) {
            lockTbl.sLock(blk);
            locks.put(blk, "S");
        }
    }

    /**
     * 为当前事务请求指定块的互斥锁。
     * <p>
     * 如果当前事务未持有共享锁，才会去为此事务请求对应块的共享锁
     * <p>
     * 假设，事务在获取到一个块的xlock前总是会先请求该块的slock
     *
     * @param blk 指定的块
     */
    public void xLock(Block blk) {
        if (!hasXLock(blk)) {
            sLock(blk);
            lockTbl.xLock(blk);
            locks.put(blk, "X");
        }

    }

    /**
     * 释放当前事务持有的所有锁。
     */
    public void release() {
        for (Block blk : locks.keySet())
            lockTbl.unLock(blk);
        locks.clear();
    }

    private boolean hasXLock(Block blk) {
        String lockType = locks.get(blk);
        return (lockType != null && lockType.equals("X"));
    }
}