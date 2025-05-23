package UI;

import DataIO.BytesIO;
import Memory.Table;

import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class MainFrame extends JFrame{

    static final int KB = 1024;

    public MainFrame(){
        setTitle("主界面");
        setSize(800, 600);
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setLocationRelativeTo(null);

        //绘制表格按钮
        initMain();
        //显示
        setVisible(true);
    }

    //绘制表格跳转按钮
    private void initMain(){
        JPanel tablePanel = new JPanel(new GridLayout(0, 1, 10, 10));

        List<String> tables = readFromFile();
        for (String table : tables) {
            JButton jButton = new JButton(table);
            jButton.addActionListener(e->{
                openTable(table);
            });
            tablePanel.add(jButton);
        }

        this.add(tablePanel);
    }

    //main.dat文件中仅存有表名   本方法将所有表名读取出来
    private List<String> readFromFile(){
        List<String> result = new ArrayList<>();
        try(BufferedInputStream bis = new BufferedInputStream(new FileInputStream("main.dat"))){
            while(true) {
                int length = bis.read();
                if(length < 0) break;
                byte[] table_name = new byte[length];
                bis.read(table_name);
                String str = new String(table_name, StandardCharsets.UTF_8);
                result.add(str);
            }
        }catch (IOException ioe){
            ioe.printStackTrace();
        }
        return result;
    }

    //启动表格系统
    private void openTable(String table_name){
        byte[] data = BytesIO.readDataInto(2*KB,0,table_name);
        new TableFrame(Table.deSerializeTable(data));
    }



    public static void main1(String[] args) {



        try(BufferedOutputStream bos =
                    new BufferedOutputStream(new FileOutputStream("main.dat"))){
            String name = "t0";
            byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
            bos.write((byte)bytes.length);
            bos.write(bytes);
        }catch (IOException ioe){
            ioe.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new MainFrame();
    }
}
