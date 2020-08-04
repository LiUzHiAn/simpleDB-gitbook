package simpledb.remote;


import java.sql.SQLException;
import java.sql.Statement;

public class SimpleConnection extends ConnectionAdapter {
    RemoteConnection rconn;

    public SimpleConnection(RemoteConnection rconn) {
        this.rconn = rconn;
    }

    @Override
    public Statement createStatement() throws SQLException {
        try {
            RemoteStatement rstmt = rconn.createStatement();
            return new SimpleStatement(rstmt);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            rconn.close();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
