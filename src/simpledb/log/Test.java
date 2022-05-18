package simpledb.log;

import java.io.IOException;

/**
 * 日志管理功能单元测试
 *
 *
 * @author shs
 * @date 2022/5/18 17:08
 */
public class Test {
    public static void main(String[] args) {
       try{
           LogManager logManager = new LogManager("first_log");
       } catch (IOException e) {
           e.printStackTrace();
       }
    }
}
