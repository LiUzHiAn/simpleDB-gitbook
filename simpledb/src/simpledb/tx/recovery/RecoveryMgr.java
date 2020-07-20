package simpledb.tx.recovery;

import simpledb.buffer.Buffer;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @ClassName RecoveryMgr
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-21 13:59
 * @Version 1.0
 **/

public class RecoveryMgr {

    private int txNum;

    /**
     * 为指定的事务创建一个恢复管理器
     *
     * @param txNum 指定事务的ID
     */
    public RecoveryMgr(int txNum) {
        this.txNum = txNum;
        new StartRecord(txNum).writeToLog();
    }

    /**
     * 写入一条提交日志记录,并将日志记录flush到日志文件
     * <p>
     * 注意，在SimpleDB中，采用的是undo-only恢复算法，
     * 因此在提交日志记录被写入前，必须将，当前事务对应的，并且是修改过的缓冲区，flush到磁盘上。
     * <p>
     * 在将缓冲区内容flush到磁盘上之前，其实还采用了write-ahead logging 技术
     * 详情参阅第14章中的算法14-7
     */
    public void commit() {
        SimpleDB.bufferMgr().flushAll(txNum);

        int lsn = new CommitRecord(txNum).writeToLog();
        SimpleDB.logMgr().flush(lsn);
    }

    public void rollback() {
        doRollback();  // 把修改后的值，再改回来
        // 为什么rollback之后还要flushAll呢？
        // 这是因为，更新日志记录在undo过程中也会修改相应的缓冲区，因此需要flush到磁盘
        SimpleDB.bufferMgr().flushAll(txNum);

        int lsn = new RollBackRecord(txNum).writeToLog();
        SimpleDB.logMgr().flush(lsn);
    }

    public void recover() {
        doRecover();  // 把修改后的值，再改回来
        SimpleDB.bufferMgr().flushAll(txNum);

        int lsn = new CheckpointRecord().writeToLog();
        SimpleDB.logMgr().flush(lsn);
    }

    /**
     * 写一条SetInt日志记录到日志文件, 并返回其LSN
     * <p>
     * 对临时文件作的修改将不被保存，此时返回的LSN是一个负值，没有意义。
     *
     * @param buffer
     * @param offset
     * @param newVal
     * @return
     */
    public int setInt(Buffer buffer, int offset, int newVal) {
        int oldVal = buffer.getInt(offset);  // 先把修改前的值记录下来，以供更新日志记录使用
        Block blk = buffer.block();

        if (isTemporaryBlock(blk))
            return -1;
        else
            return new SetIntRecord(txNum, blk, offset, oldVal).writeToLog();

    }

    /**
     * 写一条SetString日志记录到日志文件, 并返回其LSN
     * <p>
     * 对临时文件作的修改将不被保存，此时返回的LSN是一个负值，没有意义。
     *
     * @param buffer
     * @param offset
     * @param newStr
     * @return
     */
    public int setString(Buffer buffer, int offset, String newStr) {
        String oldVal = buffer.getString(offset);
        Block blk = buffer.block();

        if (isTemporaryBlock(blk))
            return -1;
        else
            return new SetStringRecord(txNum, blk, offset, oldVal).writeToLog();

    }

    /**
     * 回滚事务。
     * <p>
     * 该方法会遍历日志记录，调用遍历到的每条日志记录的undo()方法，
     * 直到该事务的START日志记录为止。
     */
    private void doRollback() {
        Iterator<LogRecord> iter = new LogRecordIterator();
        while (iter.hasNext()) {
            LogRecord rec = iter.next();
            if (rec.txNumber() == this.txNum) {
                if (rec.op() == LogRecord.START)
                    return;
                else
                    // 其实只有SetIntRecord和SetStringRecord
                    // 的undo()方法才有具体的实现，其他日志记录类
                    // 的undo()方法都是空方法。
                    rec.undo();
            }
        }

    }

    /**
     * 执行一次数据库恢复操作。
     * <p>
     * 该方法会遍历日志记录，无论何时它发现一个未完成事务的日志记录，
     * 它都会调用该日志记录的undo()方法。
     * <p>
     * 当遇到一个CHECKPOINT日志记录或日志文件尾时（从后往前读，所以实际上是文件头），
     * 恢复算法停止。
     */
    private void doRecover() {
        // 已经提交事务的ID集合
        Collection<Integer> committedTxs = new ArrayList<>();
        Iterator<LogRecord> iter = new LogRecordIterator();
        while (iter.hasNext()) {
            LogRecord rec = iter.next();
            if (rec.op() == LogRecord.CHECKPOINT)
                return;
            if (rec.op() == LogRecord.COMMIT || rec.op()==LogRecord.ROLLBACK)
                committedTxs.add(rec.txNumber());
            else if (!committedTxs.contains(rec.txNumber()))
                // 其实只有SetIntRecord和SetStringRecord
                // 的undo()方法才有具体的实现，其他日志记录类的undo()方法都是空方法。
                rec.undo();
        }
    }

    private boolean isTemporaryBlock(Block blk) {
        return blk.filename().startsWith("temp");
    }
}