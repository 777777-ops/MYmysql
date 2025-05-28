import java.io.*;
import java.util.*;

public class Trytttttt {
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

    public static void main8(String[] args) {

        Stack<String> s1 = new Stack<>();
        s1.push("a");
        s1.push("b");
        s1.push("c");
        Stack<String> s2 = new Stack<>();
        s2.addAll(s1);

    }

    public void function1() {
        // 发生RuntimeException且未处理
        throw new IllegalArgumentException("函数1的异常");
    }

    public void function2() {
        try {
            function1();
        } catch (RuntimeException e) {
            // 这里一定会执行！
            System.out.println("函数2捕获到异常: " + e.getMessage());
        }
    }

    public static void main9(String[] args) {
        new Trytttttt().function2();
    }

    public static void main(String[] args) {
        Set<Integer> s = new HashSet<>();
        s.add(8);
        s.add(8);
        s.add(8);
        s.add(8);
        s.add(8);
    }
}

