package simpledb.tx.recovery;

import simpledb.log.BasicLogRecord;
import simpledb.server.SimpleDB;

import java.util.Iterator;

import static simpledb.tx.recovery.LogRecord.*;

/**
 * @ClassName LogRecordIterator
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-21 15:37
 * @Version 1.0
 **/

public class LogRecordIterator implements Iterator<LogRecord> {
    // 先获得一个BasicLogRecord迭代器
    // 此迭代器迭代得到结果是一条条raw的日志记录
    private Iterator<BasicLogRecord> iter = SimpleDB.logMgr().iterator();


    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public LogRecord next() {
        BasicLogRecord blr = iter.next();
        int op = blr.nextInt();
        switch (op) {
            case CHECKPOINT:
                return new CheckpointRecord(blr);
            case START:
                return new StartRecord(blr);
            case COMMIT:
                return new CommitRecord(blr);
            case ROLLBACK:
                return new RollBackRecord(blr);
            case SETINT:
                return new SetIntRecord(blr);
            case SETSTRING:
                return new SetStringRecord(blr);
            default:
                return null;
        }

    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}