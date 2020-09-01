package com.xm4399.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

/**
 * @Auther: czk
 * @Date: 2020/9/1
 * @Description:
 */
public class OtherUtil {
    // 将异常信息转为string
    public  String getException(Exception e) {
        Writer writer = null;
        PrintWriter printWriter = null;
        try {
            writer = new StringWriter();
            printWriter = new PrintWriter(writer);
            e.printStackTrace(printWriter);
            return writer.toString();
        } finally {
            try {
                if (writer != null)
                    writer.close();
                if (printWriter != null)
                    printWriter.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }
}
