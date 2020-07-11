package simpledb.file;

import simpledb.server.SimpleDB;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * @ClassName Page
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-14 14:49
 * @Version 1.0
 **/

public class Page {
    public static final int BLOCK_SIZE = 400;
    public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

    public static final int STR_SIZE(int n) {
        float bytesPerChar = Charset.defaultCharset().newEncoder().maxBytesPerChar();
        // 指示字符串长度的整数 + 各字符占的字节数
        return INT_SIZE + n * ((int) bytesPerChar);
    }

    // 页中的内容
    private ByteBuffer contents = ByteBuffer.allocateDirect(BLOCK_SIZE);
    private FileMgr fileMgr = SimpleDB.fileMgr();


    public Page() {
    }

    /**
     * 文件粒度的并发锁
     */
    public synchronized int getInt(int offset) {
        contents.position(offset);
        return contents.getInt();
    }

    public synchronized void setInt(int offset, int val) {
        contents.position(offset);
        contents.putInt(val);
    }

    public synchronized String getString(int offset) {
        contents.position(offset);
        int len = contents.getInt();  // 先获取字符串的长度
        byte[] byteVal = new byte[len];  // 再获取字符串中的后续字符
        contents.get(byteVal);
        return new String(byteVal);
    }

    public synchronized void setString(int offset, String val) {
        contents.position(offset);
        byte[] byteVal = val.getBytes();
        int len = byteVal.length;  // 获取字符串的长度

        contents.putInt(len);
        contents.put(byteVal);
    }

    public synchronized void read(Block blk) {
        fileMgr.read(blk, contents);
    }

    public synchronized void write(Block blk) {
        fileMgr.write(blk, contents);
    }

    public synchronized Block append(String fileName) {
        return fileMgr.append(fileName, contents);
    }
}