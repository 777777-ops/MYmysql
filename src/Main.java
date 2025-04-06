import MyTable.Table;

import java.io.*;
import java.nio.ByteBuffer;

public class Main {
    public static void main2(String[] args) {

        BufferedOutputStream bos;
        try {
            bos = new BufferedOutputStream(new FileOutputStream("data\\try.dat"));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("找不到指定的字节文件");
        }

        try {
            bos.write((byte)0x00);
            bos.write((byte)0x00);
            bos.write((byte)0x00);
            bos.write((byte)0x00);
            bos.write((byte)0x0);

            bos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }



    }

    public static void main1(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        File currentDir = new File("");
        String path = currentDir.getAbsolutePath();
        System.out.println("当前目录: " + path);
    }
}
