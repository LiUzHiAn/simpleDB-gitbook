package simpledb.remote;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SimpleStatement extends StatementAdapter {
    RemoteStatement rstmt;

    public SimpleStatement(RemoteStatement rstmt) {
        this.rstmt = rstmt;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            RemoteResultSet rrs = rstmt.executeQuery(sql);
            return new SimpleResultSet(rrs);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            return rstmt.executeUpdateCmd(sql);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
