package simpledb.remote;

import simpledb.tx.Transaction;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * @ClassName RemoteConnectionImpl
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-03-09 19:29
 * @Version 1.0
 **/

public class RemoteConnectionImpl extends UnicastRemoteObject
        implements RemoteConnection {
    private Transaction tx;

    public RemoteConnectionImpl() throws RemoteException {
        this.tx = new Transaction();
    }

    @Override
    public RemoteStatement createStatement() throws RemoteException {
        return new RemoteStatementImpl(this);
    }

    @Override
    public void close() throws RemoteException{
        tx.commit();
    }

    //=============以下方法都是在服务端中调用的================
    Transaction getTrasnaction()
    {
        return tx;
    }
    void commit()
    {
        tx.commit();
        tx=new Transaction();
    }
    void rollback()
    {
        tx.rollback();
        tx=new Transaction();
    }
}