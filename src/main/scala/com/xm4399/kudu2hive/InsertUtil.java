package com.xm4399.kudu2hive;

import com.xm4399.util.JDBCUtil;

import java.sql.*;

/**
 * @Auther: czk
 * @Date: 2020/10/16
 * @Description:
 */
public class InsertUtil {
    public static void main(String[] args) {
        if ("myTable".equals(args[0])){
            new InsertUtil().insertCheckPointInfo();
        } else if ("myTable_hive".equals(args[0])) {
            new InsertUtil().insertMyTableHive(args[1]);
        }

    }

    public  void insertMyTableHive(String create_time)  {
        Connection connection = null;
        PreparedStatement pst =null ;
        int i = getMaxID() +1;
        int[] intArr = new int[]{20201015,20201016,20201017};
        try {
            connection = getConnection();
            while (true){
                String sql = "insert into myTable_hive (id, name,create_time) values(?,?,?)";
                pst = connection.prepareStatement(sql);
                pst.setInt(1,i);
                pst.setString(2,"xxx--" + i);
                int arrIndex = i%3;
                pst.setInt(3,Integer.parseInt(create_time));
                pst.executeUpdate();
                Thread.sleep(50);
                if (i%100 ==0){
                    System.out.println("第"+i+"次");
                }
                i ++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            close2(pst, connection);
        }
    }

    public  void insertCheckPointInfo()  {
        Connection connection = null;
        PreparedStatement pst =null ;
        int i =1;
        try {
            connection = getConnection();
            while (true){
                String sql = "insert into myTable (id, name) values(?,?)";
                pst = connection.prepareStatement(sql);
                pst.setInt(1,i);
                pst.setString(2,"xxx--" + i);
                pst.executeUpdate();
                Thread.sleep(50);
                if (i%100 ==0){
                    System.out.println("第"+i+"次");
                }
                i ++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            close2(pst, connection);
        }
    }

    public  int getMaxID() {
        Connection con = null;
        Statement stmt = null;
        ResultSet res = null;
        String sql = "select  max(id)  from myTable_hive";
        try {
            con = new JDBCUtil().getConnection();
            stmt = con.createStatement();
            res = stmt.executeQuery(sql);
            while (res.next()){
                int completedCount = res.getInt(1);
                return completedCount;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(res, stmt, con);
        }
        return  -1;
    }


    public Connection getConnection (){
        Connection connection = null;
        String address = "10.0.0.211:3307";
        String username = "gprp";
        String password = "gprp@@4399";
        String dbName = "chenzhikun_test";
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://" + address + "/" + dbName, username, password);
            return connection;
        } catch (Exception e){
            e.printStackTrace();
        }
        return  null;
    }

    /** 关闭链接,释放资源 */
    public  void close(ResultSet res, Statement stmt, Connection con ) {
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

    public  void close2(PreparedStatement pst,Connection con ) {
        try {
            if (pst != null) {
                pst.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            pst = null;
            con = null;
        }
    }

}
