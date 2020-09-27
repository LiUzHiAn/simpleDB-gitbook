package simpledb.index.btree;

import simpledb.file.Block;
import simpledb.query.Constant;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/09/27 11:25
 */
public class BTreeDir {
    private TableInfo tableInfo;
    private Transaction tx;
    private String fileName;
    private BTreePage contents;

    public BTreeDir(Block block, TableInfo tableInfo, Transaction tx) {
        this.tableInfo = tableInfo;
        this.tx = tx;
        this.fileName = block.filename();
        this.contents = new BTreePage(block, tableInfo, tx);
    }

    public void close() {
        this.contents.close();
    }

    /**
     * 搜索dataval为searchKey所在索引块的块号
     *
     * @param searchKey
     * @return
     */
    public int search(Constant searchKey) {
        Block childBlock = findChildBlock(searchKey);
        // flag只要不是0，即level-0目录块，则递归搜索
        while (contents.getFlag() > 0) {
            contents.close();
            contents = new BTreePage(childBlock, tableInfo, tx);
            childBlock = findChildBlock(searchKey);
        }
        return childBlock.number();
    }

    /**
     * 创建新的树根
     *
     * @param entry 在树根目录块中插入目录记录时引发的分块
     */
    public void makeNewRoot(DirEntry entry) {
        Constant firstVal = contents.getDataVal(0);
        int level = contents.getFlag();
        Block newBlock = contents.split(0, level);
        DirEntry oldRoot = new DirEntry(firstVal, newBlock.number());
        insertEntry(oldRoot);
        insertEntry(entry);
        contents.setFlag(level + 1);
    }

    /**
     * 插入一条目录记录，如果引发了目录块拆分，则返回一条指向该目录块的目录记录
     * @param entry
     * @return
     */
    public DirEntry insert(DirEntry entry) {
        if (contents.getFlag() == 0)
            return insertEntry(entry);
        Block childBlock = findChildBlock(entry.getDataval());
        BTreeDir child = new BTreeDir(childBlock, tableInfo, tx);
        DirEntry myEntry = child.insert(entry); // 递归
        child.close();
        // 如果子目录块产生了分块，则当前目录需要添加一个对应的目录记录
        return (myEntry != null) ? insertEntry(myEntry) : null;
    }

    // ===============私有方法==================

    /**
     * 插入一条新的目录记录
     *
     * @param entry
     * @return 如果引发了目录块拆分，则返回一个新目录块对应的目录记录
     */
    private DirEntry insertEntry(DirEntry entry) {
        int newSlot = 1 + contents.findSlotBefore(entry.getDataval());
        contents.insertDir(newSlot, entry.getDataval(), entry.getBlockNum());
        if (!contents.isFull())
            return null;
        int level = contents.getFlag();
        int splitPos = contents.getNumRecords() / 2;
        Constant splitDataval = contents.getDataVal(splitPos);
        Block newBlock = contents.split(splitPos, level);
        return new DirEntry(splitDataval, newBlock.number());
    }

    private Block findChildBlock(Constant searchKey) {
        int slotNum = contents.findSlotBefore(searchKey);

        // slotNum有两种情况：
        // 1. 找到了对应searchKey的目录记录
        // 2. 到了第一个大于等于searchKey的目录记录，但还没找到待搜索的目录记录，那肯定要往下搜索
        // 当前slotNum为匹配记录的上一条/第一个大于等于searchKey的目录记录的上一条
        if (contents.getDataVal(slotNum + 1).equals(searchKey))
            slotNum++;
        int blockNum = contents.getChild(slotNum);
        return new Block(fileName, blockNum);
    }
}
