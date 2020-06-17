package com.mycompany.app.db.controllers;

import java.sql.Connection;
import java.util.Optional;
import java.util.logging.Logger;

public class ContentDao {
    private static final Logger LOGGER = Logger.getLogger(CommitDao.class.getName());

    private final Optional<Connection> connection;

    public ContentDao(Optional<Connection> connection) throws ClassNotFoundException {
        this.connection =  connection;
    }


}
