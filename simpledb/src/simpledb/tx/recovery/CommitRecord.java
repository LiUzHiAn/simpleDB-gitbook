package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

/**
 * @ClassName CommitRecord
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-21 15:27
 * @Version 1.0
 **/

public class CommitRecord implements LogRecord {

    private int myTxNum;

    public CommitRecord(int myTxNum) {
        this.myTxNum = myTxNum;
    }

    /**
     * 根据一条BasicLogRecord来构造一条CommitRecord。
     * 该构造函数是为了给 恢复/回滚 算法调用
     * <p>
     * 注意，一条提交日志记录的格式为：
     * <p>
     * <COMMIT,txNum>
     *
     * @param blr
     */
    public CommitRecord(BasicLogRecord blr) {
        myTxNum = blr.nextInt();
    }

    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{COMMIT, myTxNum};
        LogMgr logMgr = SimpleDB.logMgr();
        return logMgr.append(rec);
    }

    @Override
    public int op() {
        return COMMIT;
    }

    @Override
    public int txNumber() {
        return myTxNum;
    }

    @Override
    public void undo(int txNum) {
        // empty is OK
    }

    public String toString() {
        return "<COMMIT " + myTxNum + ">";
    }
}