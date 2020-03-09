package simpledb.remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteResultSet extends Remote {
    public boolean next() throws RemoteException;
    public int getInt(String fieldName) throws RemoteException;
    public String getString(String fieldName) throws RemoteException;
    public RemoteMetaData getMetaData() throws RemoteException;
    public void close() throws RemoteException;
}
