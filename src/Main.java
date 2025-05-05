import Memory.ByteTools;
import Memory.IndexRecord;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Stack;

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

    public static void main(String[] args) {

        Stack<String> s1 = new Stack<>();
        s1.push("a");
        s1.push("b");
        s1.push("c");
        Stack<String> s2 = new Stack<>();
        s2.addAll(s1);

    }

}
