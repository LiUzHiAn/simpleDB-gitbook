package simpledb.log;

import simpledb.server.SimpleDB;
import simpledb.tx.recovery.LogRecord;
import simpledb.tx.recovery.LogRecordIterator;

import java.io.IOException;
import java.util.Iterator;

/**
 * @ClassName LogTest
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-16 12:28
 * @Version 1.0
 **/

public class LogTest {

    public static void main(String[] args) throws IOException {
        SimpleDB.init("studentdb");
//        LogMgr logMgr = SimpleDB.logMgr();
//        int lsn1 = logMgr.append(new Object[]{"a", "b"});
//        int lsn2 = logMgr.append(new Object[]{"c", "d"});
//        int lsn3 = logMgr.append(new Object[]{"e", "f"});
//        logMgr.flush(lsn3);

//        Iterator<BasicLogRecord> iter = logMgr.iterator();
//        while (iter.hasNext()) {
//            BasicLogRecord rec = iter.next();
//            String v1 = rec.nextString();
//            String v2 = rec.nextString();
//            System.out.println("[" + v1 + ", " + v2 + "]");
//        }

        int logRecordNum = 0;
        Iterator<LogRecord> iter = new LogRecordIterator();
        while (iter.hasNext()) {
            LogRecord record = iter.next();
            logRecordNum++;
            System.out.println(record);

        }
        System.out.println(logRecordNum);

    }
}