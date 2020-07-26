package com.xm4399.test;

import org.apache.kudu.Type;

public class TwoTypeCon {


    public Type toKuduType(String mysqlType) throws IllegalArgumentException{
        mysqlType.trim();
        String[] arr = mysqlType.split("\\(");
        if (1 == arr.length) {
            return noUnsignedToKuduType(arr[0]);
        }
        else if (2 == arr.length){
            String str2 = arr[1];
            if (str2.trim().endsWith("unsigned")) {
               return  unsignedToKuduType(arr[0]);
            }else{
                return noUnsignedToKuduType(arr[0]);
            }
        }else {
            throw new IllegalArgumentException("The provided data type doesn't map to know any known one.");
        }
        //return Type.INT64;
    }

    public Type noUnsignedToKuduType(String mysqlType) throws IllegalArgumentException {

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

    //有unsigned的情况下,将字段类型扩大.
    public Type unsignedToKuduType(String mysqlType) throws IllegalArgumentException {

        switch (mysqlType) {
            case "tinyint":
                return Type.INT16;
            case "smallint": case "mediumint":
                return Type.INT32;
            case "int": case"integer":
                return Type.INT64;
            case  "bigint":
                return Type.INT64;
            case  "float":
                return Type.FLOAT;
            case "double":
                return Type.DOUBLE;
            case "decimal":
                return Type.DECIMAL;

            default:
                throw new IllegalArgumentException("The provided data type doesn't map to know any known one.");
        }
    }


}
