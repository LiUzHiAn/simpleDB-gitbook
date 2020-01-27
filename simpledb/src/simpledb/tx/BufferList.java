package simpledb.tx;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.buffer.PageFormatter;
import simpledb.file.Block;
import simpledb.server.SimpleDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName BufferList
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-27 21:38
 * @Version 1.0
 **/

public class BufferList {

    private Map<Block, Buffer> buffers = new HashMap<>();
    // 已经的所有块
    private List<Block> pins = new ArrayList<>();
    private BufferMgr bufferMgr = SimpleDB.bufferMgr();

    Buffer getBuffer(Block blk) {
        return buffers.get(blk);
    }

    void pin(Block blk) {
        Buffer buff = bufferMgr.pin(blk);
        buffers.put(blk, buff);
        pins.add(blk);
    }

    Block pinNew(String fileName, PageFormatter pfmt) {
        Buffer buff = bufferMgr.pinNew(fileName, pfmt);
        Block blk = buff.block();
        buffers.put(blk, buff);
        pins.add(blk);
        return blk;
    }

    void unpin(Block blk) {
        Buffer buff = buffers.get(blk);
        bufferMgr.unpin(buff);
        pins.remove(blk);

        // 如果块取消固定后，固定次数为0，则从缓存区map中移除该entry
        if (!pins.contains(blk))
            buffers.remove(blk);
    }

    void unpinAll() {
        for (Block blk : pins) {
            Buffer buff = buffers.get(blk);
            bufferMgr.unpin(buff);
        }

        buffers.clear();
        pins.clear();
    }
}