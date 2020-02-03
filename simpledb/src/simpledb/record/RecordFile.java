package simpledb.record;

import simpledb.file.Block;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @ClassName RecordFile
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/3 7:58 下午
 * @Version 1.0
 */
public class RecordFile {
    private TableInfo tableInfo;
    private Transaction tx;
    private String fileName;
    private RecordPage recordPage;
    private int currentBlkNum;

    public RecordFile(TableInfo tableInfo, Transaction tx) throws IOException {
        this.tableInfo = tableInfo;
        this.tx = tx;
        this.fileName = tableInfo.fileName();
        // 如果记录文件当前为空，则新追加一个块
        if (tx.size(fileName) == 0)
            appendBlock();
        moveTo(0);  // 移动到第0个块
    }

    public void close() {
        recordPage.close();
    }

    // 涉及移动当前记录位置的操作
    public void beforeFirst() {
        moveTo(0);
    }

    /**
     * 判断是否存在下一条记录
     *
     * @return
     * @throws IOException
     */
    public boolean next() throws IOException {
        while (true) {
            // 有下一条记录
            if (recordPage.next())
                return true;
            // 没有下一条记录，并且是在最后一个块
            if (atLastBlock())
                return false;
            // 没有下一条记录，但是不是最后一个块
            moveTo(currentBlkNum + 1); // 移动到下一块
        }
    }

    public void moveToRID(RID rid) {
        moveTo(rid.blockNumber()); // 先移到指定块
        recordPage.moveToID(rid.id()); // 再移到块内指定ID的记录处
    }

    /**
     * 插入一个记录。
     * <p>
     * 注意，插入总是成功的。
     * 1. 要么是在已有的块中找到一个空位置
     * 2. 要么是追加了一个新的块
     * <p>
     * 新记录的起始位置都可以由recordPage.currentPos()方法获取到。
     * <p>
     * 此外，这里的插入其实只修改了记录的 EMPTY/INUSE 标志字节，
     * 客户端代码需要紧接着修改实际的值。
     *
     * @throws IOException
     */
    public void insert() throws IOException {
        // 当始终找不到一个插入的位置时
        while (!recordPage.insert()) {
            // 如果现在是在最后一个块，那肯定是要追加一个新块了
            if (atLastBlock())
                appendBlock();
            // 否则不断往后面的块里面找
            moveTo(currentBlkNum + 1);
        }
    }

    // 访问当前记录具体数据的方法
    public int getInt(String fldname) {
        return recordPage.getInt(fldname);
    }

    public String getString(String fldname) {
        return recordPage.getString(fldname);
    }

    public void setInt(String fldname, int newVal) {
        recordPage.setInt(fldname, newVal);
    }

    public void setString(String fldname, String newVal) {
        recordPage.setString(fldname, newVal);
    }

    public RID currentRID() {
        return new RID(currentBlkNum, recordPage.currentID());
    }

    public void delete() {
        recordPage.delete();
    }

    /**
     * 移动至指定块
     *
     * @param specificBlkNum 指定的块
     */
    private void moveTo(int specificBlkNum) {
        // 主动释放掉缓冲区中已经固定的，对应当前记录块的缓冲区
        if (recordPage != null) {
            recordPage.close();
        }
        currentBlkNum = specificBlkNum;
        Block blk = new Block(fileName, currentBlkNum);
        recordPage = new RecordPage(blk, tableInfo, tx);
    }

    /**
     * 追加一个新的记录块
     */
    private Block appendBlock() {
        RecordFormatter recordFormatter = new RecordFormatter(tableInfo);
        return tx.append(fileName, recordFormatter);
    }

    /**
     * 判断当前是否在记录文件的最后一块
     *
     * @return
     * @throws IOException
     */
    private boolean atLastBlock() throws IOException {
        return currentBlkNum == tx.size(fileName) - 1;
    }
}
