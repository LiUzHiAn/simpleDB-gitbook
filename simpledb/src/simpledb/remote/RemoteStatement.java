package simpledb.remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteStatement extends Remote {
    public RemoteResultSet executeQuery(String qry) throws RemoteException;
    public int executeUpdateCmd(String cmd) throws RemoteException;
}
