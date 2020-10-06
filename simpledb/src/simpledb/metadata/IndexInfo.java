package simpledb.metadata;

import simpledb.index.btree.BTreeIndex;
import simpledb.index.hash.HashIndex;
import simpledb.index.Index;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;

import static simpledb.file.Page.BLOCK_SIZE;

/**
 * @ClassName IndexInfo
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/6 12:42 下午
 * @Version 1.0
 */
public class IndexInfo {
    private String idxName;
    private String fieldName;
    private Transaction tx;
    private TableInfo tableInfo;
    private StatInfo statInfo;

    /**
     * 创建一个索引
     *
     * @param idxName   索引名
     * @param tblName   被索引的表名
     * @param fieldName 被索引的字段名，当前只支持单字段索引
     * @param tx
     */
    public IndexInfo(String idxName, String tblName, String fieldName, Transaction tx) throws IOException {
        this.idxName = idxName;
        this.fieldName = fieldName;
        this.tx = tx;
        tableInfo = SimpleDB.metadataMgr().getTableInfo(tblName, tx);
        statInfo = SimpleDB.metadataMgr().getStatInfo(tblName, tx);
    }

    /**
     * 搜索索引所需访问的块数。
     * <p>
     * 注意，在SimpleDB中只支持一个字段的索引
     * 不像在表记录文件中，索引不需要一个 EMPTY/INUSE flag
     *
     * @return
     */
    public int blocksAccessed() {
        // 被索引的记录，每条的长度
        int recordLen = tableInfo.recordLength();
        // 一个块可以存储多少条索引
        int recordNumPerBlock = BLOCK_SIZE / recordLen;
        // 总块数
        int numBlocks = statInfo.recordsOutput() / recordNumPerBlock;

        return BTreeIndex.searchCost(numBlocks, recordNumPerBlock);
    }

    public int recordsOutput() {
        return statInfo.recordsOutput()
                / statInfo.distinctValues(fieldName);
    }

    public int distinctValues(String fieldName) {
        if (fieldName.equals(this.fieldName))
            return 1;
        else
            return Math.min(statInfo.distinctValues(fieldName), recordsOutput());
    }

    public Index open() throws IOException {
        Schema sch = schema();
        return new BTreeIndex(idxName, sch, tx);
    }

    /**
     * 构造索引的schema，如下：
     * <p>
     * (block, id, dataval)
     *
     * @return
     */
    private Schema schema() {
        Schema sch = new Schema();
        sch.addIntField("block");
        sch.addIntField("id");
        // 索引字段是 int 类型字段
        if (tableInfo.schema().type(fieldName) == Schema.INTEGER)
            sch.addIntField("dataval");
            // 索引字段是 string 类型字段
        else {
            int fieldLen = tableInfo.schema().length(fieldName);
            sch.addStringField("dataval", fieldLen);
        }
        return sch;
    }
}
