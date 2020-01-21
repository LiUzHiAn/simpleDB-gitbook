package simpledb.tx.recovery;

import com.sun.org.apache.regexp.internal.RE;
import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.log.BasicLogRecord;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

/**
 * @ClassName SetStringRecord
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-21 14:29
 * @Version 1.0
 **/

public class SetStringRecord implements LogRecord {
    private int myTxNum;
    private int offset;

    private String oldVal;
    private Block blk;

    public SetStringRecord(int myTxNum, Block blk, int offset, String oldVal) {
        this.myTxNum = myTxNum;
        this.offset = offset;
        this.blk = blk;
        this.oldVal = oldVal;
    }

    /**
     * 根据一条BasicLogRecord来构造一条SetStringRecord。
     * 该构造函数是为了给 恢复/回滚 算法调用
     * <p>
     * 注意，一条更新日志记录的格式为：
     * <p>
     * <SETxxx,txNum,fileName,blkNum,offset,old value,new value>
     *
     * @param blr
     */
    public SetStringRecord(BasicLogRecord blr) {
        myTxNum = blr.nextInt();
        String fileName = blr.nextString();
        int blkNum = blr.nextInt();
        blk = new Block(fileName, blkNum);
        offset = blr.nextInt();
        oldVal = blr.nextString();
    }

    /**
     * 将一条日志记录写入日志文件，返回LSN
     *
     * @return
     */
    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{SETSTRING, myTxNum,
                blk.filename(), offset, oldVal};

        LogMgr logMgr = SimpleDB.logMgr();
        return logMgr.append(rec);
    }

    /**
     * 返回日志记录的操作符。
     * <p>
     * CHECKPOINT = 0, START = 1,
     * COMMIT = 2, ROLLBACK = 3,
     * SETINT = 4, SETSTRING = 5;
     *
     * @return integer
     */
    @Override
    public int op() {
        return SETSTRING;
    }

    @Override
    public int txNumber() {
        return myTxNum;
    }

    @Override
    public void undo(int txNum) {
        BufferMgr bufferMgr = SimpleDB.bufferMgr();
        Buffer buff = bufferMgr.pin(blk);
        buff.setString(offset, oldVal, myTxNum, -1);
        bufferMgr.unpin(buff);
    }

    public String toString() {
        return "<SETSTRING " + myTxNum + " " + blk + " " + offset
                + " " + oldVal + ">";
    }

}