package simpledb.index.btree;

import simpledb.file.Block;
import simpledb.query.Constant;
import simpledb.record.RID;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/09/27 09:13
 */
public class BTreeLeaf {

    private TableInfo tableInfo;
    private String fileName;
    private Constant searchKey;
    private BTreePage contents;
    private int currentSlot;
    private Transaction tx;

    public BTreeLeaf(Block block, TableInfo tableInfo, Constant searchKey, Transaction tx) {
        this.tableInfo = tableInfo;
        this.contents = new BTreePage(block, tableInfo, tx);
        this.fileName = block.filename();
        this.searchKey = searchKey;
        this.currentSlot = this.contents.findSlotBefore(searchKey);
        this.tx = tx;
    }

    public void close() {
        this.contents.close();
    }

    /**
     * 是否存在下一条dataval为this.searchKey的索引记录
     *
     * @return
     */
    public boolean next() {
        currentSlot++;  // todo: important here
        if (currentSlot >= contents.getNumRecords())
            return tryOverflow();
        else if (contents.getDataVal(currentSlot).equals(searchKey))
            return true;
        else
            return tryOverflow();
    }

    /**
     * 返回当前索引记录的RID
     *
     * @return
     */
    public RID getDataRID() {
        return this.contents.getDataRID(currentSlot);
    }

    public void delete(RID datarid) {
        while (next()) {
            if (getDataRID().equals(datarid)) {
                contents.delete(currentSlot);
                return;
            }
        }
    }

    /**
     * 插入一条新的索引记录，可能引发块拆分
     *
     * @param datarid 新索引记录的datarid
     * @return 如果引发块拆分，则返回一个目录记录对象
     */
    public DirEntry insert(RID datarid) {
        currentSlot++;
        contents.insertLeaf(currentSlot, searchKey, datarid);
        if (!contents.isFull()) {
            return null;
        }
        // 如果插入后满了，需要拆分
        Constant firstKey = contents.getDataVal(0);
        Constant lastKey = contents.getDataVal(contents.getNumRecords() - 1);

        if (firstKey.equals(lastKey))  // 如果都是同样dataval的记录
        {
            Block newBlock = contents.split(1, contents.getFlag());
            contents.setFlag(newBlock.number());  // flag标志位设置为溢出配块的块号
            return null;
        } else {
            int splitPos = contents.getNumRecords() / 2;  // 从中间位置开始拆分
            Constant splitKey = contents.getDataVal(splitPos);
            if (splitKey.equals(firstKey)) {
                // 中间往右搜索，直到遇到下一个和splitKey不同的dataval
                while (contents.getDataVal(splitPos).equals(splitKey))
                    splitPos++;
                splitKey = contents.getDataVal(splitPos);
            } else {
                // 中间往左搜素，直到遇到下一个和splitKey不同的dataval
                while (contents.getDataVal(splitPos - 1).equals(splitKey))
                    splitPos--;
            }
            Block newBlock = contents.split(splitPos, -1);  // todo: flag设置为-1,表示普通的索引块
            return new DirEntry(splitKey, newBlock.number());  // splitKey 和 块号组成一条 目录记录
        }
    }

    /**
     * 判断是否存在溢出块
     *
     * @return
     */
    private boolean tryOverflow() {
        Constant firstKey = contents.getDataVal(0);
        int flag = contents.getFlag();
        if (!searchKey.equals(firstKey) || flag < 0)
            return false;
        contents.close();
        Block nextBlock = new Block(fileName, flag);  // 下一块
        contents = new BTreePage(nextBlock, tableInfo, tx);
        currentSlot = 0;
        return true;
    }
}
