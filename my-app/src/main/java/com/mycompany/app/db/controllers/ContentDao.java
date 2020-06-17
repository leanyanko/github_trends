package com.mycompany.app.db.controllers;

import com.mycompany.app.db.JDBCConnect;

import java.sql.Connection;
import java.util.Optional;
import java.util.logging.Logger;

public class ContentDao {
    private static final Logger LOGGER = Logger.getLogger(PostgresDao.class.getName());

    private final Optional<Connection> connection;

    public ContentDao(Optional<Connection> connection) throws ClassNotFoundException {
        this.connection =  connection;
    }


}
