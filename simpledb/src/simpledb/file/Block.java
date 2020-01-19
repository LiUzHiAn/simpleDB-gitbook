package simpledb.file;

/**
 * @ClassName simpledb.file.Block
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-14 14:45
 * @Version 1.0
 **/

public class Block {
    private String fileName;
    private int blkNum;

    public Block(String fileName, int blkNum) {
        this.fileName = fileName;
        this.blkNum = blkNum;
    }

    public String filename() {
        return fileName;
    }

    public int number() {
        return blkNum;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        Block blk = (Block) obj;
        return fileName.equals(blk.fileName) && blkNum == blk.blkNum;
    }

    @Override
    public String toString() {
        return "[File: " + fileName + ", block number: " + blkNum + "]";
    }
}