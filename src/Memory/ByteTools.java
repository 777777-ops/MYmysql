package Memory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

//工具类
public class ByteTools {
    //常量
    public static final byte TYPE_NULL = 0x00;       //为空标志
    public static final byte TYPE_STRING = 0x01;     //字符串类型
    public static final byte TYPE_INT = 0x02;        //整数类型
    public static final byte TYPE_FLOAT = 0x03;      //小数类型
    public static final byte TYPE_BOOLEAN = 0x04;    //布尔类型

    //拼接两个字节数组
    public static byte[] concatBytes(byte[] b1,byte[] b2){
        int len1 = b1.length;
        int len2 = b2.length;
        byte[] newBytes = new byte[len1 + len2];
        int index = 0;
        for (byte b : b1) newBytes[index++] = b;
        for (byte b : b2) newBytes[index++] = b;
        return newBytes;
    }


    //根据数据类型返回相应的数据类型常量字节
    public static byte typeToByte(Class<?> type){
        if(type == String.class)
            return TYPE_STRING;
        else if(type == Integer.class)
            return TYPE_INT;
        else if(type == Float.class)
            return TYPE_FLOAT;
        else if(type == Boolean.class)
            return TYPE_BOOLEAN;
        else{
            throw new RuntimeException("无法找到该类型对应的字节");
        }
    }

    //根据数据类型常量字节放回相应的数据类型
    public static Class<?> byteToType(byte b){
        return switch (b) {
            case TYPE_STRING -> String.class;
            case TYPE_INT -> Integer.class;
            case TYPE_FLOAT -> Float.class;
            case TYPE_BOOLEAN -> Boolean.class;
            default -> throw new RuntimeException("无法根据该字节标识返回相应的数据类型");
        };
    }

    //根据数据类型返回相应的单数据储存所需空间
    public static short singleObjectLength(Class<?> type,int length){
        int sum;
        if(type == String.class) {sum = 1 + 2 + length * 4;}
        else if(type == Integer.class){sum = 1 + 4;}
        else if(type == Float.class){sum = 1 + 4;}
        else if(type == Boolean.class){sum = 1 + 1;}
        else{
            throw new RuntimeException("无法找到该类型对应的字节");
        }
        return (short)sum;
    }

    //int类型数据转为3字节数据
    public static byte[] intTo3Bytes(int value) {
        // 检查范围（无符号 0 ~ 16,777,215）
        if (value < 0 || value > 0xFFFFFF) {
            throw new IllegalArgumentException("Value must fit in 24 bits (0 ~ 16,777,215)");
        }

        byte[] bytes = new byte[3];
        bytes[0] = (byte) ((value >> 16) & 0xFF); // 最高字节
        bytes[1] = (byte) ((value >> 8)  & 0xFF); // 中间字节
        bytes[2] = (byte) (value & 0xFF);         // 最低字节
        return bytes;
    }

    //将3字节数据转回int类型
    public static int threeBytesToInt(byte[] bytes) {
        if (bytes.length != 3) {
            throw new IllegalArgumentException("Byte array must be of length 3");
        }

        // 将每个 byte 转为无符号 int，再按位组合
        return ((bytes[0] & 0xFF) << 16)  // 最高字节左移 16 位
                | ((bytes[1] & 0xFF) << 8)   // 中间字节左移 8 位
                | (bytes[2] & 0xFF);         // 最低字节
    }

    //单个数据反序列化，将单个数据从字节数组中提取出来，并且移动了buffer游标
    protected static Object deSerializeSingleObject(ByteBuffer buffer,byte type,short length) {  //传入该字段的类型
        switch (type) {
            case (TYPE_NULL):
                return null;
            case (TYPE_STRING):
                int str_length = buffer.getShort();                          //实际字符串长度
                int bytes_length = length * 4;    //总字符串长度
                byte[] strBytes = new byte[str_length];
                buffer.get(strBytes);
                if(length != (short) 0)                                          //有时即使是字符串length也为0 （）
                    buffer.get(new byte[bytes_length - str_length]);             //使游标跑完剩下的字符串空闲空间
                return (new String(strBytes, StandardCharsets.UTF_8));
            case (TYPE_INT):
                return buffer.getInt();
            case (TYPE_FLOAT):
                return buffer.getFloat();
            case (TYPE_BOOLEAN):
                return buffer.get() == 1;
            default:
                throw new RuntimeException("反序列化IndexRecord时，检测到非法字段类型");
        }
    }

    //单个数据序列化 将一个Object转化为字节数组   该Object只能是规定的几个数据类型   (要移动到Tools)
    protected static byte[] serializeSingleObject(Object obj, Class<?> field,short length)  //字段数据，字段数据类型，字段定长
    {
        //如果字段数据为null
        if (obj == null)
            return new byte[]{TYPE_NULL};   //返回空标志
        //创建一个新字节数组
        byte[] bytes;
        ByteBuffer buffer;

        if (field == String.class)   //字符串
        {
            byte[] strBytes = ((String) obj).getBytes(StandardCharsets.UTF_8);
            short bytes_length =(short) (length * 4);       //该字符串所需要占用的最大字节空间 即最大字符长度 * 4
            if(strBytes.length > 500) throw new RuntimeException("?"+"该次插入数据时发生错误,本程序尚不支持长文本数据存储");
            buffer = ByteBuffer.allocate(1 + 2 + bytes_length)
                    .put(TYPE_STRING)
                    .putShort((short)strBytes.length)         //最大能记录100字符长度的字符串  //要限制
                    .put(strBytes);
        } else if (field == Integer.class) {
            buffer = ByteBuffer.allocate(1 + 4)
                    .put(TYPE_INT)
                    .putInt((Integer) obj);
        } else if (field == Float.class) {
            buffer = ByteBuffer.allocate(1 + 4)
                    .put(TYPE_FLOAT)
                    .putFloat((Float) obj);
        } else if (field == Boolean.class) {
            buffer = ByteBuffer.allocate(1 + 1)
                    .put(TYPE_BOOLEAN)
                    .put((byte) (((boolean) obj) ? 1 : 0));
        } else {
            throw new RuntimeException("非法类型");
        }

        bytes = buffer.array();
        return bytes;
    }

}
