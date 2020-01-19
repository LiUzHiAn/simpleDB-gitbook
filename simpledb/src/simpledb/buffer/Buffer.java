package simpledb.buffer;

import simpledb.file.Block;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

/**
 * @ClassName Buffer
 * @Deacription
 * // TODO 以后实现了事务模块后，需要再修改事务相关代码
 * @Author LiuZhian
 * @Date 2020-01-17 19:29
 * @Version 1.0
 **/

public class Buffer {
    private Page contents = new Page();
    private Block blk = null;
    // 当前缓冲页固定的次数，有点多线程中ReentranLock的味道
    private int pins = 0;
    // 和事务相关 TODO
    private int modifiedBy = -1;
    private int logSequenceNum = -1;

    public int getInt(int offset) {
        return contents.getInt(offset);
    }

    public String getString(int offset) {
        return contents.getString(offset);
    }

    public void setInt(int offset, int val, int txNum, int LSN) {
        // 和事务相关 TODO
        modifiedBy = txNum;
        // LSN的当前实现就是日志文件块的块号
        if (LSN >= 0)
            logSequenceNum = LSN;
        contents.setInt(offset, val);
    }

    public void setString(int offset, String val, int txNum, int LSN) {
        // 和事务相关 TODO
        modifiedBy = txNum;
        // LSN的当前实现就是日志文件块的块号
        if (LSN >= 0)
            logSequenceNum = LSN;
        contents.setString(offset, val);
    }

    public Block block() {
        return blk;
    }

    /**
     * 将缓冲页中的内容写回到磁盘，且在写回数据到磁盘前，追加一条日志记录。
     * <p>
     * 此方法有点类似OS中处理内存的脏读位（dirty-read）的行为，当脏读位为1，即发生了时，
     * OS会先将内存中的值写回文件块，再执行后续的磁盘文件读入内存的操作。
     */
    void flush() {
        // 如果有修改页中内容：
        // 1. 先flush一下日志记录,
        // 2. 再将内存页的内容写回磁盘
        if (modifiedBy >= 0) {
            SimpleDB.logMgr().flush(logSequenceNum);
            contents.write(blk);
        }
    }

    void pin() {
        pins++;
    }

    void unpin() {
        pins--;
    }

    boolean isPinned() {
        return pins > 0;
    }

    boolean isModifyiedBy(int txNum) {
        return txNum == modifiedBy;
    }

    /**
     * 将缓冲页中的内容写回磁盘。
     * <p>
     * 注意，在写回磁盘前，要检查下当前页的内容是否被修改过！
     * 如果被修改过，必须先在写回磁盘前写一条日志记录，然后再执行磁盘写操作。
     * <p>
     * 有点类似Buffer类的构造函数。
     *
     * @param b 待写回的块
     */
    void assignToBlock(Block b) {
        flush();
        blk = b;
        contents.read(blk);
        pins = 0;
    }

    /**
     * 将缓冲页的内容格式化，再追加到文件块
     * <p>
     * 注意，在追加磁盘块前，也要检查下当前页的内容是否被修改过！
     * 如果被修改过，必须先在写回磁盘前写一条日志记录，
     * 然后再执行缓冲页的格式化操作，再追加回磁盘块中。
     *
     * @param fileName
     * @param pfm
     */
    void assignToNew(String fileName, PageFormatter pfm) {
        flush();
        pfm.format(contents);
        blk = contents.append(fileName);
        pins = 0;
    }
}