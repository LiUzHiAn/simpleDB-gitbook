package simpledb.tx.recovery;

/**
 * @ClassName LogRecord
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-21 14:20
 * @Version 1.0
 **/

public interface LogRecord {

    static final int CHECKPOINT = 0, START = 1,
            COMMIT = 2, ROLLBACK = 3,
            SETINT = 4, SETSTRING = 5;
    int writeToLog();
    int op();
    int txNumber();
    void undo();
}