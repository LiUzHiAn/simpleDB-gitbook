package simpledb.index.btree;

import simpledb.file.Block;
import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.IntConstant;
import simpledb.query.StringConstant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.function.IntConsumer;

import static simpledb.record.Schema.INTEGER;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/09/27 14:37
 */
public class BTreeIndex implements Index {
    private Transaction tx;
    private TableInfo leafTableInfo, dirTableInfo;
    private BTreeLeaf leaf = null;
    private Block rootBlock;

    public BTreeIndex(String idxName, Schema leafSchema, Transaction tx) throws IOException {
        this.tx = tx;

        String leafTableName = idxName + "leaf";
        leafTableInfo = new TableInfo(leafTableName, leafSchema);
        if (tx.size(leafTableInfo.fileName()) == 0) {
            // -1 表示叶子结点
            tx.append(leafTableInfo.fileName(), new BTreePageFormatter(leafTableInfo, -1));
        }

        // 构建目录相关对象
        Schema dirSchema = new Schema();
        // 只用到了leafSchema中的dataval和block字段
        dirSchema.add("block", leafSchema);
        dirSchema.add("dataval", leafSchema);
        String dirTableName = idxName + "dir";
        dirTableInfo = new TableInfo(dirTableName, dirSchema);
        rootBlock = new Block(dirTableInfo.fileName(), 0);  // 树根块
        if (tx.size(dirTableInfo.fileName()) == 0) {
            // 最开始，目录块的level为0
            tx.append(dirTableInfo.fileName(), new BTreePageFormatter(dirTableInfo, 0));
            BTreePage page = new BTreePage(rootBlock, dirTableInfo, tx);
            // 插入一条目录记录，指向leafTable的block 0
            int fieldType = dirSchema.type("dataval");
            Constant minVal = null;
            if (fieldType == INTEGER)
                minVal = new IntConstant(Integer.MIN_VALUE);
            else
                minVal = new StringConstant("");
            page.insertDir(0, minVal, 0);   // 指向leafTable的block 0
            page.close();
        }
    }

    @Override
    public void beforeFirst(Constant searchKey) throws IOException {
        close();
        BTreeDir root = new BTreeDir(rootBlock, dirTableInfo, tx);  // 树根
        int leafBlockNum = root.search(searchKey);
        root.close();
        Block leafBlock = new Block(leafTableInfo.fileName(), leafBlockNum);
        leaf = new BTreeLeaf(leafBlock, leafTableInfo, searchKey, tx);
    }

    @Override
    public boolean next() throws IOException {
        return leaf.next();
    }

    @Override
    public RID getDataRid() {
        return leaf.getDataRID();
    }

    @Override
    public void insert(Constant dataval, RID datarid) throws IOException {
        beforeFirst(dataval);
        DirEntry entry = leaf.insert(datarid);
        leaf.close();
        // 如果entry不为null，说明有索引块拆分，从而引发了这条新的目录记录
        if (entry == null)
            return;
        BTreeDir root = new BTreeDir(rootBlock, dirTableInfo, tx);
        // 如果entry2不为null，说明有目录块拆分，从而引发了这条新的目录记录
        // 由于当前的目录块为root，如果有拆分，那需要重新创建树根
        DirEntry entry2 = root.insert(entry);
        if(entry2!=null)
            root.makeNewRoot(entry2);
        root.close();
    }

    @Override
    public void delete(Constant dataval, RID datarid) throws IOException {
        beforeFirst(dataval);
        leaf.delete(datarid);
        leaf.close();
    }

    @Override
    public void close() {
        if (leaf != null)
            leaf.close();
    }

    public static int searchCost(int numblocks, int rpb) {
        return 1 + (int)(Math.log(numblocks) / Math.log(rpb));
    }
}
