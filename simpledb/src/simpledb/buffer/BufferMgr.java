package simpledb.buffer;

import simpledb.file.Block;


/**
 * @ClassName BufferMgr
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-17 19:31
 * @Version 1.0
 **/

public class BufferMgr {

    // 最长等待时间
    private static final long MAX_TIME = 10000;

    private BasicBufferMgr basicBufferMgr;

    public BufferMgr(int numBuffers) {
        basicBufferMgr = new BasicBufferMgr(numBuffers);
    }

    /**
     * 对BasicBufferMgr类中pin方法的包装
     *
     * @param blk
     * @return
     */
    public synchronized Buffer pin(Block blk) {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buff = basicBufferMgr.pin(blk);
            while (null == buff && !waitTooLong(timestamp)) {
                // this.wait()，等待的对象是当前这个缓冲管理器，
                // 等待的目标是有一个未被固定的缓冲区
                wait(MAX_TIME);
                buff = basicBufferMgr.pin(blk);
            }
            // TODO 这里非常重要
            // 如果因为死锁或其他各种原因，导致等待时间超过阈值 MAX_TIM，则向客户端抛出异常，让客户端自行处理
            if (null == buff)
                throw new BufferAbortException();

            return buff;
        } catch (InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    public synchronized void unpin(Buffer buffer) {
        basicBufferMgr.unpin(buffer);
        // 一旦有一个缓冲区“自由”，通知其他所有在缓冲管理器对象上等待的线程
        if (!buffer.isPinned())
            notifyAll();
    }

    /**
     * 对BasicBufferMgr类中pinNew方法的包装
     *
     * @param fileName
     * @param pageFormatter
     * @return
     */
    public synchronized Buffer pinNew(String fileName, PageFormatter pageFormatter) {
        try {
            long timestamp=System.currentTimeMillis();
            Buffer buff = basicBufferMgr.pinNew(fileName, pageFormatter);
            while (null == buff && !waitTooLong(timestamp)) {
                // this.wait()，等待的对象是当前这个缓冲管理器，
                // 等待的目标是有一个未被固定的缓冲区
                wait(MAX_TIME);
                buff = basicBufferMgr.pinNew(fileName, pageFormatter);
            }
            // TODO 这里非常重要
            // 如果发生了死锁
            if (null == buff)
                throw new BufferAbortException();

            return buff;
        } catch (InterruptedException e) {
            throw new BufferAbortException();
        }
    }

    public void flushAll(int txNum) {
        basicBufferMgr.flushAll(txNum);
    }

    public int available() {
        return basicBufferMgr.available();
    }


    private boolean waitTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }
}