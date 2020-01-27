package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

/**
 * @ClassName CheckpointRecord
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-21 15:32
 * @Version 1.0
 **/

public class CheckpointRecord implements LogRecord {


    public CheckpointRecord() {

    }

    /**
     * 根据一条BasicLogRecord来构造一条CheckpointRecord。
     * 该构造函数是为了给 恢复/回滚 算法调用
     * <p>
     * 注意，一条提交日志记录的格式为：
     * <p>
     * <CHECKPOINT>
     *
     * @param blr
     */
    public CheckpointRecord(BasicLogRecord blr) {

    }

    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{CHECKPOINT};
        LogMgr logMgr = SimpleDB.logMgr();
        return logMgr.append(rec);
    }

    @Override
    public int op() {
        return CHECKPOINT;
    }

    /**
     * Checkpoint 日志记录没有对应的事务ID
     *
     * @return -1，a dummy value
     */
    @Override
    public int txNumber() {
        return -1;
    }

    @Override
    public void undo() {
        // empty is Okay
    }

    public String toString() {
        return "<CHECKPOINT>";
    }
}