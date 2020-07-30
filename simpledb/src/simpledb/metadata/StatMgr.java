package simpledb.metadata;

import simpledb.record.RecordFile;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName StatMgr
 * @Description TODO
 * @Author LiuZhian
 * @Date 2020/2/5 9:36 下午
 * @Version 1.0
 */
public class StatMgr {

    // 每执行100次检索表的数据统计元数据后，刷新一次数据库统计信息
    public static final int REFRESH_EVERY_CALLS = 100;
    private Map<String, StatInfo> tableStats;
    private int numCalls;
    private TableMgr tableMgr;


    public StatMgr(TableMgr tableMgr, Transaction tx) throws IOException {
        this.tableMgr = tableMgr;
        // 新建对象时统计一次
        refreshStatistics(tx);
    }

    public synchronized StatInfo getStatInfo(String tableName, Transaction tx) throws IOException {
        this.numCalls++;
        if (numCalls > REFRESH_EVERY_CALLS)
            refreshStatistics(tx);
        StatInfo statInfo = tableStats.get(tableName);

        // 如果某个客户端在刚好完成上一次所有表的数据统计后，创建了一个新的表
        // 而这时所有表的数据统计信息又还没到下一次重新计算的时候，那么可能会暂时查不到结果

        // 这时，我们再显示地执行一次查询指定表的数据统计元数据操作。
        if (null == statInfo) {
            refreshTableStats(tableName, tx);
            statInfo = tableStats.get(tableName);
        }
        return statInfo;
    }

    /**
     * 更新所有表的数据统计信息
     *
     * @param tx
     * @throws IOException
     */
    public synchronized void refreshStatistics(Transaction tx) throws IOException {
        tableStats = new HashMap<>();
        this.numCalls = 0;
        // 先获得tblcat表，获取各表的信息
        TableInfo tblCatTableInfo = tableMgr.getTableInfo("tblcat", tx);
        RecordFile tblCatRecordFile = new RecordFile(tblCatTableInfo, tx);

        // 更新每张表的数据统计信息
        // TODO 新建一个RecordFile对象时会自动移到第一块，所以以下语句可以省略
        // tblCatRecordFile.beforeFirst();
        // TODO
        while (tblCatRecordFile.next()) {
            String tblName = tblCatRecordFile.getString("tblname");
            // 更新具体的某张表的统计信息
            refreshTableStats(tblName, tx);
        }
        tblCatRecordFile.close();
    }

    /**
     * 更新指定表的数据统计信息
     *
     * @param tblName
     * @param tx
     * @throws IOException
     */
    private synchronized void refreshTableStats(String tblName, Transaction tx) throws IOException {
        int numRecords = 0;
        TableInfo tableInfo = tableMgr.getTableInfo(tblName, tx);
        RecordFile recordFile = new RecordFile(tableInfo, tx);
        // TODO 新建一个RecordFile对象时会自动移到第一块，所以以下语句可以省略
        recordFile.beforeFirst();
        // TODO
        while (recordFile.next()) {
            numRecords++;
        }
        // 这个时候，recordFile肯定遍历到了表记录文件的最后一块，
        // 因此可以求得总块号（块号是从0开始的）
        int numBlocks = recordFile.currentRID().blockNumber() + 1;
        recordFile.close();

        // 新建出表的数据统计信息，即块数+ 记录条数
        StatInfo statInfo = new StatInfo(numBlocks, numRecords);
        this.tableStats.put(tblName, statInfo);

    }

}
