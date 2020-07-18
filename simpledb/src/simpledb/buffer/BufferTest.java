package simpledb.buffer;

import simpledb.file.Block;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

import java.io.IOException;

/**
 * @ClassName BufferTest
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-17 19:40
 * @Version 1.0
 **/

public class BufferTest {

    public static void main(String[] args) throws IOException {
        // Case 1
        SimpleDB.init("lzadb");
        BufferMgr bufferMgr = SimpleDB.bufferMgr();
        Block blk = new Block("junk", 3);
        Buffer buffer = bufferMgr.pin(blk);
        int n = buffer.getInt(392);
        String str = buffer.getString(20);
        // 客户端代码需要对unpin负责
        bufferMgr.unpin(buffer);
        System.out.println("Values are: " + n + ", " + str);


//        // Case 2
//        SimpleDB.init("studentdb");
//        BufferMgr bufferMgr = SimpleDB.bufferMgr();
//        Block blk = new Block("junk", 3);
//        Buffer buffer = bufferMgr.pin(blk);
//        int n = buffer.getInt(392);
//
//        LogMgr logMgr = SimpleDB.logMgr();
//        int myTxNum = 1; // 假设这里有个事务标识符1
//        Object[] logRec = new Object[]{"junk", 3, 392, n};
//        int LSN=logMgr.append(logRec);
//
//        buffer.setInt(392,n+1,myTxNum,LSN);
//        // 客户端代码需要对unpin负责
//        bufferMgr.unpin(buffer);

        // Case 3
//        SimpleDB.init("lzadb");
//        BufferMgr bufferMgr = SimpleDB.bufferMgr();
//        PageFormatter pf=new ABCStringFormatter();
//        Buffer buffer = bufferMgr.pinNew("junk",pf);
//
//        String str=buffer.getString(0);
//        assert (str.equals("abc"));
//
//        int blkNum=buffer.block().number();
//        System.out.println("Appended block number: "+blkNum);


    }
}