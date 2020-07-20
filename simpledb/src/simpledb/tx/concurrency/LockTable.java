package simpledb.tx.concurrency;

import simpledb.file.Block;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName LockTable
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-25 17:21
 * @Version 1.0
 **/

public class LockTable {

    private static final long MAX_TIME = 10000; // 10 s
    private Map<Block, Integer> locks = new HashMap<>();

    /**
     * 请求持有指定块的 共享锁
     *
     * @param blk 指定的块
     */
    public synchronized void sLock(Block blk) {
        try {
            long timestamp = System.currentTimeMillis();
            // 当该块的互斥锁已经被持有时，线程等待
            while (hasXLock(blk) && !waitTooLong(timestamp))
                // this.wait()，等待的对象是当前这个锁表，
                // 等待的目标是该块的互斥锁被释放
                wait(MAX_TIME);

            // 死锁或其他原因导致 等待超时
            if (hasXLock(blk))
                throw new LockAbortException();
            // 这个val肯定是个非负的值
            // 1. 如果这个块之前没有访问过，即lockVal=0
            // 2. 如果这个块的共享锁已经被持有，则lockVal > 0
            int val = getLockVal(blk);
            locks.put(blk, val + 1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    /**
     * 请求持有指定块的 互斥锁
     * <p>
     * 我们假定事务已经获取互斥锁前，都会获得该块的 共享锁。（即locks对应 blk的entry值至少为1）
     *
     * @param blk 指定的块
     */
    public synchronized void xLock(Block blk) {
        try {
            long timestamp = System.currentTimeMillis();
            // 当该块的共享锁已经被其他事务持有时，线程等待
            while (hasOtherSLocks(blk) && !waitTooLong(timestamp))
                // this.wait()，等待的对象是当前这个锁表，
                // 等待的目标是该块的共享锁被释放
                wait(MAX_TIME);

            // 死锁或其他原因导致等待超时
            if (hasOtherSLocks(blk))
                throw new LockAbortException();

            locks.put(blk, -1); // 获得互斥锁，把锁表置为-1
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }


    public synchronized void unLock(Block blk) {
        int val = getLockVal(blk);
        if (val > 1)  // 多于一个客户端持有块的共享锁
            locks.put(blk, val - 1);
        else { // 某客户端拥有该块的共享锁 或 互斥锁
            locks.remove(blk);
            // 该块变得空闲，通知所有等待线程竞争
            notifyAll();
        }
    }

    /**
     * 判断指定块的互斥锁是否已经被占用。
     * <p>
     * getLockVal(block) 为 -1 时表示互斥锁被占用。
     *
     * @param block
     * @return
     */
    private boolean hasXLock(Block block) {
        return getLockVal(block) < 0;
    }


    /**
     * 判断指定块的共享锁是否已经被持有。
     * <p>
     * getLockVal(block) 为 被持有的次数。
     *
     * @param block
     * @return
     */
    private boolean hasOtherSLocks(Block block) {
        return getLockVal(block) > 1;
    }

    private int getLockVal(Block block) {
        Integer val = locks.get(block);
        if (null == val)
            return 0;
        return val;
    }

    private boolean waitTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }
}