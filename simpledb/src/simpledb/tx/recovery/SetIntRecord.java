package simpledb.tx.recovery;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.log.BasicLogRecord;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

/**
 * @ClassName SetIntReord
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-21 15:06
 * @Version 1.0
 **/

public class SetIntRecord implements LogRecord {

    private int myTxNum;
    private int offset;
    private int oldVal;
    private Block blk;


    public SetIntRecord(int myTxNum, Block blk, int offset, int oldVal) {
        this.myTxNum = myTxNum;
        this.blk = blk;
        this.offset = offset;
        this.oldVal = oldVal;
    }

    /**
     * 根据一条BasicLogRecord来构造一条SetIntRecord。
     * 该构造函数是为了给 恢复/回滚 算法调用
     * <p>
     * 注意，一条更新日志记录的格式为：
     * <p>
     * <SETxxx,txNum,fileName,blkNum,offset,old value,new value>
     *
     * @param blr
     */
    public SetIntRecord(BasicLogRecord blr) {
        myTxNum = blr.nextInt();
        String filename = blr.nextString();
        int blkNum = blr.nextInt();
        blk = new Block(filename, blkNum);
        offset = blr.nextInt();
        oldVal = blr.nextInt();
    }

    @Override
    public int writeToLog() {

        Object[] rec = new Object[]{SETINT, myTxNum, blk.filename(),
                blk.number(), offset, oldVal};

        LogMgr logMgr = SimpleDB.logMgr();
        return logMgr.append(rec);
    }

    @Override
    public int op() {
        return SETINT;
    }

    @Override
    public int txNumber() {
        return myTxNum;
    }

    @Override
    public void undo(int txNum) {
        BufferMgr bufferMgr = SimpleDB.bufferMgr();
        Buffer buffer = bufferMgr.pin(blk);
        buffer.setInt(offset, oldVal, myTxNum, -1);
        bufferMgr.unpin(buffer);
    }

    public String toString() {
        return "<SETINT " + myTxNum + " " + blk + " " + offset + " " + oldVal + ">";
    }
}