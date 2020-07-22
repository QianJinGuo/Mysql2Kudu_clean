package com.xm4399.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCInsertRecords {

    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/chenzhikun";
    static final String USER = "canal";
    static final String PASS = "canal";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try{
            //STEP 2: Register JDBC driver
            Class.forName("com.mysql.cj.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");

            //STEP 4: Execute a query
            System.out.println("Inserting records into the table...");
            stmt = conn.createStatement();

            String sql = "INSERT INTO chenzhikun_test_3 " +
                    "VALUES (1,\"a\",\"a\",1,1,1,\"a\")";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO chenzhikun_test_3 " +
                    "VALUES (2,\"b\",\"b\",2,2,2,\"b\")";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO chenzhikun_test_3 " +
                    "VALUES (3,\"c\",\"c\",3,3,3,\"c\")";
            stmt.executeUpdate(sql);
            /*sql = "INSERT INTO student " +
                    "VALUES(103, 'Java', 'Ja', 28)";
            stmt.executeUpdate(sql);*/
            System.out.println("Inserted records into the table...");

        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }finally{
            //finally block used to close resources
            try{
                if(stmt!=null)
                    conn.close();
            }catch(SQLException se){
            }// do nothing
            try{
                if(conn!=null)
                    conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }//end finally try
        }//end try
        System.out.println("Goodbye!");
    }//end main
}//end JDBCExample//原文出自【易百教程】，商业转载请联系作者获得授权，非商业请保留原文链接：https://www.yiibai.com/jdbc/jdbc-insert-records.html




