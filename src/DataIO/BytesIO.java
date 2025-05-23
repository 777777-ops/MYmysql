package DataIO;

import java.io.IOException;
import java.io.RandomAccessFile;

public class BytesIO {

    //根据表名返回表文件所在的路径
    public static String tableFile(String table_name) {
        StringBuilder fileway = new StringBuilder(table_name);
        fileway.append(".dat");
        fileway.insert(0,"table\\\\");
        return fileway.toString();
    }

    //打开文件写入数据             (table_name是为了查找文件)
    public static void writeDataOut(byte[] data,int off_set,String table_name){
        String file = tableFile(table_name);
        try(RandomAccessFile raf = new RandomAccessFile(file,"rw")){
            //定位到要写入数据的位置
            raf.seek(off_set);
            //写入数据
            raf.write(data);
        }catch (IOException e){
            System.out.println("文件操作失败:"+e.getMessage());
        }
    }

    //打开文件读出数据       (已知数据长短)
    public static byte[] readDataInto(int length,int off_set,String table_name){
        String file = tableFile(table_name);
        byte[] data = new byte[length];
        try(RandomAccessFile raf = new RandomAccessFile(file,"r")){
            //定位到要读出数据的位置
            raf.seek(off_set);
            //读入数据
            raf.read(data);
        }catch (IOException e){
            System.out.println("文件操作失败:"+e.getMessage());
        }
        return data;
    }

    //打开文件根据位置返回一个raf对象       (未知数据长短)    (用完该函数一定要.clear())
    public static RandomAccessFile readDataIntoUnknown(int off_set, String table_name){
        String file = tableFile(table_name);
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file,"r");
            //定位到要读出数据的位置
            raf.seek(off_set);
            //返回该随机位置指针
            return raf;
        }catch (IOException e){
            System.out.println("返回RandomAccessFile类失败:"+e.getMessage());
        }
        return null;/*?*/
    }
}
