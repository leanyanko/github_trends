package com.mycompany.app.db;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JDBCConnect {

    private Connection con = null;

    public Connection getConnection(String credentials, String user, String pwd) throws ClassNotFoundException{
        Class.forName("org.postgresql.Driver");
        if (con == null) {
            try {

                con = DriverManager.getConnection(credentials, user, pwd);
                System.out.println("Connected to the database!");

            } catch (SQLException e) {
                System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return con;
    }
    public void connect (String credentials) throws Exception {

        String[] c = credentials.split(" ");
        Class.forName("org.postgresql.Driver");
        try (Connection conn = DriverManager.getConnection( c[0], c[1], c[2])) {

            if (conn != null) {
                System.out.println("Connected to the database!");
            } else {
                System.out.println("Failed to make connection!");
            }

        } catch (SQLException e) {
            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
