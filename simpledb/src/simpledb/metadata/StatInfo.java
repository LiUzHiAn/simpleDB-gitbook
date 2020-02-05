package simpledb.metadata;

/**
 * @ClassName StatInfo
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/5 8:52 下午
 * @Version 1.0
 */
public class StatInfo {
    private int numblocks;
    private int numRecords;

    public StatInfo(int numblocks, int numRecords) {
        this.numblocks = numblocks;
        this.numRecords = numRecords;
    }

    public int blocksAccessed() {
        return numblocks;
    }

    public int recordOutput() {
        return numRecords;
    }

    /**
     * 统计某个字段的所有取值的数量。
     *
     * 当前仅用（ 记录数/3 + 1 ）来模拟。
     * @param filedName
     * @return
     */
    public int distinctValues(String filedName) {
        // TODO
        // TODO 实际统计每个字段的取值情况
        return 1 + (numRecords / 3);
    }
}
