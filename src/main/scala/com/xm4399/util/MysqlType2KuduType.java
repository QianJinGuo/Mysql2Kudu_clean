package com.xm4399.util;

import org.apache.kudu.Type;

public class MysqlType2KuduType {

    public  Type toKuduType(String mysqlType) throws IllegalArgumentException {

        switch (mysqlType) {
            case "tinyint":
                return Type.INT8;
            case "smallint":
                return Type.INT16;
            case "int": case"integer": case "mediumint":
                return Type.INT32;
            case  "bigint":
                return Type.INT64;
            case  "float":
                return Type.FLOAT;
            case "double":
                return Type.DOUBLE;
            case "decimal":
                return Type.DECIMAL;

            case "date": case "time": case "year": case "datetime":   case "timestamp":
                return Type.STRING;

            case "varchar": case  "char": case "tinytext":
             case "text": case "mediumtext":  case "longtext":
                return Type.STRING;

            case "tinyblob": case "blob": case "mediumblob": case "longblob" :
                return Type.BINARY;

            default:
                throw new IllegalArgumentException("The provided data type doesn't map to know any known one.");
        }
    }
}
