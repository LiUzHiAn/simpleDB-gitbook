package simpledb.log;

import simpledb.file.Block;
import simpledb.file.Page;

import java.util.Iterator;

import static simpledb.file.Page.INT_SIZE;
import static simpledb.log.LogMgr.LAST_POS;

/**
 * @ClassName LogIterator
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-16 16:26
 * @Version 1.0
 **/

public class LogIterator implements Iterator<BasicLogRecord> {

    private Block blk;
    private Page page = new Page();
    private int currentRec;  // 迭代器当前遍历的日志记录结束位置

    public LogIterator(Block blk) {
        this.blk = blk;
        page.read(blk);
        // 初始化为最后一条日志记录的结束位置
        currentRec = page.getInt(LAST_POS);
    }

    @Override
    public boolean hasNext() {
        return currentRec > 0 || blk.number() > 0;
    }

    @Override
    public BasicLogRecord next() {
        if (0 == currentRec)
            moveToNextBlock();
        // 继续往回迭代上一条
        currentRec = page.getInt(currentRec);
        return new BasicLogRecord(page, currentRec + INT_SIZE);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove operation is not supported in LogIterator！");
    }

    private void moveToNextBlock() {
        // 上一个块
        blk = new Block(blk.filename(), blk.number() - 1);
        page.read(blk);
        currentRec = page.getInt(LAST_POS);
    }
}