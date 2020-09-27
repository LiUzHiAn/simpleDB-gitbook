package simpledb.index.btree;

import simpledb.query.Constant;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/09/27 11:30
 */
public class DirEntry {
    // 目录记录的dataval
    private Constant dataval;
    // 如果当前目录记录的level为0，那么blockNum为索引块的块号
    // 如果当前目录记录的level大于0，那么blockNum为level-1级目录块的块号
    private int blockNum;

    public DirEntry(Constant dataval, int blockNum) {
        this.dataval = dataval;
        this.blockNum = blockNum;
    }

    public Constant getDataval() {
        return dataval;
    }

    public void setDataval(Constant dataval) {
        this.dataval = dataval;
    }

    public int getBlockNum() {
        return blockNum;
    }

    public void setBlockNum(int blockNum) {
        this.blockNum = blockNum;
    }
}
