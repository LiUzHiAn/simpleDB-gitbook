package simpledb.file;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName FileMgr
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-14 14:51
 * @Version 1.0
 **/

public class FileMgr {
    private File dbDirectory;
    private boolean isNew;
    private Map<String, FileChannel> openFiles = new HashMap<>();

    public FileMgr(String dbname) {
        // 默认路径为user的home路径
        String homedir = System.getProperty("user.home");
        dbDirectory = new File(homedir, dbname);
        isNew = !dbDirectory.exists();

        // 如果是新的数据库，则创建
        if (isNew && !dbDirectory.mkdir())
            throw new RuntimeException("Cannot create " + dbname);
        // 删除临时表文件
        for (String filename : dbDirectory.list()) {
            if (filename.startsWith("temp"))
                new File(dbDirectory, filename).delete();
        }

    }

    public boolean isNew() {
        return isNew;
    }

    /**
     * 返回当前文件的逻辑块数目
     *
     * @param fileName 文件名
     * @return 逻辑块数
     * @throws IOException
     */
    public int size(String fileName) throws IOException {
        FileChannel fc = getFile(fileName);
        int sz = (int) fc.size() / Page.BLOCK_SIZE;

        return sz;
    }

    private synchronized FileChannel getFile(String fileName) throws IOException {
        FileChannel fc = openFiles.get(fileName);
        // 如果map中没打开过
        if (fc == null) {
            File dbTable = new File(dbDirectory, fileName);
            RandomAccessFile f = new RandomAccessFile(dbTable, "rws");
            fc = f.getChannel();
            openFiles.put(fileName, fc);
        }

        return fc;
    }

    synchronized void read(Block block, ByteBuffer buffer) {
        try {
            buffer.clear();
            FileChannel fc = getFile(block.filename());
            fc.read(buffer, block.number() * buffer.capacity());
        } catch (IOException e) {
            throw new RuntimeException("Cannot read block " + block);
        }

    }

    synchronized void write(Block blk, ByteBuffer buffer) {
        try {
            buffer.rewind();
            FileChannel fc = getFile(blk.filename());
            fc.write(buffer, blk.number() * buffer.capacity());
        } catch (IOException e) {
            throw new RuntimeException("Cannot write block " + blk);
        }
    }

    /**
     * 追加一个新块到文件尾部，并将缓冲区的内容写入
     *
     * @param filename 待追加的文件名
     * @param buffer   缓冲区，其中保存了要追加的内容
     * @return 新建的块
     */
    synchronized Block append(String filename, ByteBuffer buffer) {
        try {
            int newBlkNum = size(filename);
            Block newBlk = new Block(filename, newBlkNum);
            write(newBlk, buffer);
            return newBlk;
        } catch (IOException e) {
            throw new RuntimeException("Cannot append block to file " + filename);
        }
    }
}