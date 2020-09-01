package com.xm4399.util;

import java.sql.*;

/**
 * @Auther: czk
 * @Date: 2020/8/30
 * @Description:
 */
public class JDBCErrorLogAndCheckUtil {

    public  void insertCheckInfo(String jobID, Boolean countResult, Boolean sampleResult)  {
        Connection connection = null;
        PreparedStatement pst =null ;
        try {
            connection = getConnection();
            String sql = "insert into check_result (job_id, count_result,sample_result) values(?,?,?)";
            pst = connection.prepareStatement(sql);
            int  jobIDNum = Integer.parseInt(jobID);
            String countResultStr = String.valueOf(countResult);
            String sampleResultStr = String.valueOf(sampleResult);
            pst.setInt(1,jobIDNum);
            pst.setString(2,countResultStr);
            pst.setString(3,sampleResultStr);
            pst.executeUpdate();
            pst.close();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                pst.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                pst = null;
                connection = null;
            }
        }
    }


    //写入报错信息
    public  void insertErroeInfo(String jobID, String jobPart, String errorMsg)  {
        Connection connection = null;
        PreparedStatement pst =null ;
        try {
            connection = getConnection();
            String sql = "insert into error_log (job_id, job_part, error_msg) values(?,?,?)";
            pst = connection.prepareStatement(sql);
            int  jobIDNum = Integer.parseInt(jobID);
            pst.setInt(1,jobIDNum);
            pst.setString(2,jobPart);
            pst.setString(3,errorMsg);
            pst.executeUpdate();
            pst.close();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                pst.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
               pst = null;
               connection = null;
            }
        }
    }

    public static Connection getConnection (){
        Connection connection = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://10.0.0.211:3307/chenzhikun_test", "gprp", "gprp@@4399");
            //connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/chenzhikun", "canal", "canal");
            return connection;
        } catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    /** 关闭链接,释放资源 */
    public static void close(ResultSet res, Statement stmt, Connection con ) {
        try {
            if (res != null) {
                res.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            res = null;
            stmt = null;
            con = null;
        }
    }
}
