package simpledb.remote;

import simpledb.query.Plan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * @ClassName RemoteStatementImpl
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-03-14 20:11
 * @Version 1.0
 **/

public class RemoteStatementImpl extends UnicastRemoteObject
        implements RemoteStatement {
    private RemoteConnectionImpl rconn;

    public RemoteStatementImpl(RemoteConnectionImpl rconn) throws RemoteException {
        this.rconn = rconn;
    }

    @Override
    public RemoteResultSet executeQuery(String qry) throws RemoteException {
        try {
            Transaction tx = rconn.getTransaction();
            Plan plan = SimpleDB.planner().createQueryPlan(qry, tx);
            return new RemoteResultSetImpl(plan, rconn);
        } catch (IOException e) {
            rconn.rollback();
            System.out.println("Error in RemoteStatementImpl executeQuery() method!");
            return null;
        }
    }

    @Override
    public int executeUpdateCmd(String cmd) throws RemoteException {
        try {
            Transaction tx = rconn.getTransaction();
            int result = SimpleDB.planner().executeUpdate(cmd, tx);
            rconn.commit();
            return result;
        } catch (IOException e) {
            rconn.rollback();
            System.out.println("Error in RemoteStatementImpl executeUpdateCmd() method!");
            return -1;
        }
    }

}