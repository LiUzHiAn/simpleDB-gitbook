package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.PageFormatter;
import simpledb.file.Block;
import simpledb.server.SimpleDB;
import simpledb.tx.concurrency.ConcurrencyMgr;
import simpledb.tx.recovery.RecoveryMgr;

import java.awt.print.PageFormat;
import java.io.IOException;

/**
 * @ClassName Transaction
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-19 19:19
 * @Version 1.0
 **/

public class Transaction {
    private static int nextTxNum = 0;
    private RecoveryMgr recoveryMgr;
    private ConcurrencyMgr concurMgr;
    private int txNum;
    private BufferList myBuffers = new BufferList();

    public Transaction() {
        txNum = nextTxNumber();
        recoveryMgr = new RecoveryMgr(txNum);
        concurMgr = new ConcurrencyMgr();
    }

    public int getTxNum() {
        return txNum;
    }

    public void commit() {
        recoveryMgr.commit();
        System.out.println("transaction " + txNum + " committed");
        myBuffers.unpinAll();
        concurMgr.release();
    }

    public void rollback() {
        recoveryMgr.rollback();
        System.out.println("transaction " + txNum + " rolled back");
        myBuffers.unpinAll();
        concurMgr.release();
    }

    public void recover() {
        SimpleDB.bufferMgr().flushAll(txNum);
        recoveryMgr.recover();
    }

    public void pin(Block blk) {
        myBuffers.pin(blk);
    }

    public void unpin(Block blk) {
        myBuffers.unpin(blk);
    }

    public int getInt(Block blk, int offset) {
        concurMgr.sLock(blk);
        // 客户端自己负责在调用getInt()方法前固定指定块
        Buffer buff = myBuffers.getBuffer(blk);
        return buff.getInt(offset);
    }

    public String getString(Block blk, int offset) {
        concurMgr.sLock(blk);
        Buffer buff = myBuffers.getBuffer(blk);
        return buff.getString(offset);
    }

    public void setInt(Block blk, int offset, int val) {
        concurMgr.xLock(blk);
        Buffer buff = myBuffers.getBuffer(blk);
        // 返回追加一条日志记录后的LSN
        int lsn = recoveryMgr.setInt(buff, offset, val);
        buff.setInt(offset, val, txNum, lsn);
    }

    public void setString(Block blk, int offset, String val) {
        concurMgr.xLock(blk);
        Buffer buff = myBuffers.getBuffer(blk);
        // 返回追加一条日志记录后的LSN
        int lsn = recoveryMgr.setString(buff, offset, val);
        buff.setString(offset, val, txNum, lsn);
    }

    /**
     * 获取文件的大小，即块的数量
     *
     * @param fileName 指定文件名
     * @return 块数
     * @throws IOException
     */
    public int size(String fileName) throws IOException {
        // 模拟的文件EOF
        Block dummyBlk = new Block(fileName, -1);
        concurMgr.sLock(dummyBlk);
        return SimpleDB.fileMgr().size(fileName);
    }

    public Block append(String fileName, PageFormatter pfmt) {
        // 模拟的文件EOF
        Block dummyBlk = new Block(fileName, -1);
        concurMgr.xLock(dummyBlk);

        Block blk = myBuffers.pinNew(fileName, pfmt);
        unpin(blk);
        return blk;
    }

    private static synchronized int nextTxNumber() {
        nextTxNum++;
        System.out.println("new transaction: " + nextTxNum);
        return nextTxNum;
    }

}