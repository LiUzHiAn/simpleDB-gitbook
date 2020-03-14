package simpledb.remote;

import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * @ClassName RemoteResultSetImpl
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-03-14 20:21
 * @Version 1.0
 **/

public class RemoteResultSetImpl extends UnicastRemoteObject
        implements RemoteResultSet {
    private RemoteConnectionImpl rconn;
    private Scan s;
    private Schema sch;

    public RemoteResultSetImpl(Plan plan, RemoteConnectionImpl rconn)
            throws RemoteException {
        try {
            s = plan.open();
            sch = plan.schema();
            this.rconn = rconn;
        } catch (IOException e) {
            System.out.println("Error in RemoteResultSetImpl constructor!");
        }

    }

    @Override
    public boolean next() throws RemoteException {
        try {
            return s.next();
        } catch (IOException e) {
            rconn.rollback();
            System.out.println("Error in RemoteResultSetImpl next() method!");
        }
        return false;
    }

    @Override
    public int getInt(String fieldName) throws RemoteException {
        // 大小写不敏感
        fieldName=fieldName.toLowerCase();
        return s.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) throws RemoteException {
        // 大小写不敏感
        fieldName=fieldName.toLowerCase();
        return s.getString(fieldName);
    }

    @Override
    public RemoteMetaData getMetaData() throws RemoteException {
        return new RemoteMetaDataImpl(sch);
    }

    @Override
    public void close() throws RemoteException {
        try {
            s.close();
            rconn.commit();
        } catch (IOException e) {
            rconn.rollback();
            System.out.println("Error in RemoteResultSetImpl close() method!");
        }

    }
}