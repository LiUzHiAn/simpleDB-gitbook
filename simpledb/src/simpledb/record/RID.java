package simpledb.record;

/**
 * @ClassName RID
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/3 7:58 下午
 * @Version 1.0
 */
public class RID {
    private int blkNum;
    private int id;

    public RID(int blkNum, int id) {
        this.blkNum = blkNum;
        this.id = id;
    }

    public int blockNumber() {
        return blkNum;
    }

    public int id() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        RID rid = (RID) obj;
        return blkNum == rid.blkNum && id == rid.id;
    }
}
