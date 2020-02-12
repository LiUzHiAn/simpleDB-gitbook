package simpledb.query;

import simpledb.record.RID;

import java.io.IOException;

public interface UpdateScan extends Scan {
    public void setVal(String fieldName,Constant newVal);
    public void setInt(String fieldName,int newVal);
    public void setString(String fieldName,String newVal);
    public void insert() throws IOException;
    public void delete();

    public RID getRID();
    public void moveToRId(RID rid);
}
