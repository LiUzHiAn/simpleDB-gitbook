package simpledb.planner;

import simpledb.parser.*;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * @ClassName UpdatePlanner
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-02-26 18:09
 * @Version 1.0
 **/

public interface UpdatePlanner {
    public int executeInsert(InsertData insertData, Transaction tx) throws IOException;
    public int executeDelete(DeleteData deleteData,Transaction tx) throws IOException;
    public int executeModify(ModifyData modifyData,Transaction tx) throws IOException;

    public int executeCreateTable(CreateTableData data,Transaction tx) throws IOException;
    public int executeCreateView(CreateViewData data, Transaction tx) throws IOException;
    public int executeCreateIndex(CreateIndexData data,Transaction tx) throws IOException;
}