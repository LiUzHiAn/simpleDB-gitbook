package simpledb.remote;

import simpledb.record.Schema;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import static simpledb.record.Schema.INTEGER;
import static simpledb.record.Schema.VARCHAR;

/**
 * @ClassName RemoteMetaDataImpl
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-03-14 20:32
 * @Version 1.0
 **/

public class RemoteMetaDataImpl extends UnicastRemoteObject
        implements RemoteMetaData {
    private Schema schema;
    private Object[] fieldNames;

    public RemoteMetaDataImpl(Schema schema) throws RemoteException {
        this.schema = schema;
        fieldNames = schema.fields().toArray();
    }

    @Override
    public int getColumnCount() throws RemoteException {
        return fieldNames.length;
    }

    @Override
    public String getColumnName(int column) throws RemoteException {
        return (String) fieldNames[column];
    }

    @Override
    public int getColumnType(int column) throws RemoteException {
        String fieldName = getColumnName(column);
        return schema.type(fieldName);
    }

    @Override
    public int getColumnDisplaySize(int column) throws RemoteException {
        String fieldName = getColumnName(column);
        int fieldType = schema.type(fieldName);
        // 字符串长度
        if (fieldType == VARCHAR)
            return schema.length(fieldName);
        else
            return 6;  // 6位数来显示一个整数（可以不一样）
    }
}