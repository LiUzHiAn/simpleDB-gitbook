package simpledb.remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteMetaData extends Remote {
    public int getColumnCount() throws RemoteException;
    public  String getColumnName(int column) throws RemoteException;
    public int getColumnType(int column) throws RemoteException;
    public int getColumnDisplaySize(int column) throws RemoteException;
}
