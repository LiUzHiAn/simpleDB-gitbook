package simpledb.buffer;

import simpledb.file.Block;

/**
 * @ClassName BasicBufferMgr
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-17 22:03
 * @Version 1.0
 **/

public class BasicBufferMgr {

    private Buffer[] bufferPool;
    private int numAvailable;

    public BasicBufferMgr(int numBuffers) {
        this.numAvailable = numBuffers;
        bufferPool = new Buffer[numAvailable];
        for (int i = 0; i < numBuffers; i++) {
            bufferPool[i] = new Buffer();
        }
    }

    synchronized void flushAll(int txNum) {
        for (Buffer buffer : bufferPool) {
            if (buffer.isModifyiedBy(txNum)) {
                buffer.flush();
            }
        }
    }

    /**
     * 固定一个块的内容到一个缓冲区中
     *
     * @param blk 待固定的块
     * @return 固定成功的缓冲区对象 或 null（表示需要等待）
     */
    synchronized Buffer pin(Block blk) {
        Buffer buff = findExistingBuffer(blk);
        // 1. 如果没有缓冲区的内容就是待关联的块,则找一个没被固定的缓冲区
        if (null == buff) {
            buff = chooseUnpinnedBuffer();
            // 1.1 如果不存在没被固定的缓冲区，则返回null
            if (null == buff)
                return null;
            // 1.2 找到了一个没被固定的块，则将块的值赋上
            buff.assignToBlock(blk);
        }
        // 2. 如果存在一个缓冲区的内容就是待关联的块
        // 2.1 如果该缓冲区没被固定,则需要先固定一下，其实就是减少一下
        if (!buff.isPinned())
            numAvailable--;
        // 2.1 固定的次数自增1  && 2.2 如果已经固定了，则固定的次数自增1
        buff.pin();

        return buff;
    }

    /**
     * 以指定的格式化器，格式化缓冲区中页的内容，并将内容追加一个新块到文件末尾。
     *
     * @param fileName 文件名
     * @param pfmt     指定的格式化器
     * @return 固定成功的缓冲区对象 或 null（表示需要等待）
     */
    synchronized Buffer pinNew(String fileName, PageFormatter pfmt) {
        Buffer buff = chooseUnpinnedBuffer();
        if (null == buff)
            return null;
        buff.assignToNew(fileName, pfmt);
        numAvailable--;
        buff.pin();

        return buff;
    }

    /**
     * 减少一个缓冲区的固定次数
     * <p>
     * 注意，减少一次后不一定这个页就“自由了”
     *
     * @param buff 待取消固定的缓冲区
     */
    synchronized void unpin(Buffer buff) {
        buff.unpin();
        if (!buff.isPinned())
            numAvailable++;

    }

    /**
     * 得到“自由的”缓冲区数量
     *
     * @return int
     */
    int available() {
        return numAvailable;
    }

    /**
     * 查找是否存在一个缓冲区，其内容块引用与待查找的一致。
     *
     * @param block 待查找的块引用
     * @return 如果存在，就返回那个缓冲区；否则返回null
     */
    private Buffer findExistingBuffer(Block block) {
        for (Buffer buff : bufferPool) {
            Block blkInBuffer = buff.block();
            if (blkInBuffer != null && blkInBuffer.equals(block)) {
                return buff;
            }
        }
        return null;
    }

    /**
     * 在缓冲池中找一个没被固定的页
     *
     * @return 如果存在，就返回那个缓冲区；否则返回null
     */
    private Buffer chooseUnpinnedBuffer() {
        for (Buffer buff : bufferPool) {
            if (!buff.isPinned()) {
                return buff;
            }
        }
        return null;
    }
}