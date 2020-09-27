package simpledb.index.hash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @program simpledb
 * @description: TODO
 * @author: liuzhian
 * @create: 2020/09/09 23:07
 */
public class HashIndex implements Index {
    private static final int NUM_BUCKETS = 100;
    private Constant searchKey;
    private String idxName;  // 索引名
    private Schema schema;   // 索引表对应的schema信息
    private Transaction tx;
    private TableScan tableScan;   // 每个桶对应的tableScan对象

    public HashIndex(String idxName, Schema schema, Transaction tx) {
        this.idxName = idxName;
        this.schema = schema;
        this.tx = tx;
    }

    @Override
    public void beforeFirst(Constant searchKey) throws IOException {
        close();  // 如果有的话，先关闭之前的桶
        this.searchKey = searchKey;
        int bucket = searchKey.hashCode() % NUM_BUCKETS;  // 简单的求模作为hash函数
        String specificBucketTableName = idxName + bucket;
        TableInfo tableInfo = new TableInfo(specificBucketTableName, schema);
        tableScan = new TableScan(tableInfo, tx);
    }

    @Override
    public boolean next() throws IOException {
        while (tableScan.next()) {
            if (tableScan.getVal("dataval").equals(searchKey))
                return true;
        }
        return false;
    }

    @Override
    public RID getDataRid() {
        // 当前索引记录对应的数据记录的RID
        int blockNum = tableScan.getInt("block");
        int recordId = tableScan.getInt("id");
        return new RID(blockNum, recordId);
    }

    @Override
    public void insert(Constant dataval, RID datarid) throws IOException {
        beforeFirst(dataval);
        tableScan.insert();  // 找到索引桶文件的插入位置
        // 填写索引记录中的dataval 和 datarid 部分取值
        tableScan.setVal("dataval", dataval);
        tableScan.setInt("block", datarid.blockNumber());
        tableScan.setInt("id", datarid.id());

    }

    @Override
    public void delete(Constant dataval, RID datarid) throws IOException {
        beforeFirst(dataval);
        while (next()) {
            if (getDataRid().equals(datarid)) {
                tableScan.delete();
                return;
            }
        }
    }

    @Override
    public void close() {
        if (tableScan != null) {
            tableScan.close();
        }
    }

    /**
     * @description: 返回遍历索引的搜索代价
     * @param num_blocks: 索引的总块数
     * @param rbp: 一个块中可以存放多少条索引记录
     * @return: int
     * @author: liuzhian
     * @date: 2020/9/9
     */
    public static int searchCost(int num_blocks,int rbp) {
        return num_blocks / NUM_BUCKETS;
    }
}
