package simpledb.tx;

import simpledb.buffer.PageFormatter;
import simpledb.file.Block;

import java.awt.print.PageFormat;

/**
 * @ClassName Transaction
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-19 19:19
 * @Version 1.0
 **/

public class Transaction {

    public Transaction();
    public void commit();
    public void rollback();
    public void recover();

    public void pin(Block blk);
    public void unpin(Block blk);
    public int getInt(Block blk,int offset);
    public String getString(Block blk,int offset);
    public void setInt(Block blk,int offset,int val);
    public void setString(Block blk,int offset,String val);

    public int size(String fileName);
    public Block append(String fileName, PageFormatter pfmt);

}