package simpledb.file;

import simpledb.server.SimpleDB;


/**
 * @ClassName FileTest
 * @Deacription // TODO
 * @Author LiuZhian
 * @Date 2020-01-14 20:02
 * @Version 1.0
 **/

public class FileTest {

    public static void main(String[] args) {
        try {
            SimpleDB.init("studentdb");

            // 第0块
            Block blk = new Block("junk", 0);
            Page p1 = new Page();
            p1.read(blk);
            // 将第0块的第105字节开始的int整数加1
            int n = p1.getInt(105);
            assert (n == 0);
            p1.setInt(105, n + 1);
            p1.write(blk);

            // 重新读回第0块
            Page p2 = new Page();
            p2.read(blk);
            int added_n = p2.getInt(105);
            assert (added_n == n + 1);

            // 追加另一个block （即 block 1）
            Page p3 = new Page();
            p3.setString(20, "hola");
            blk = p3.append("junk");

            // 再次重新读回追加的block到内存
            Page p4 = new Page();
            p4.read(blk);
            String s = p4.getString(20);
            assert (s.equals("hola"));

            // 追加另一个block （即 block 2）
            Page p5 = new Page();
            p5.setInt(88, 1);
            blk = p5.append("junk");
            assert (p5.getInt(88) == 1);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}