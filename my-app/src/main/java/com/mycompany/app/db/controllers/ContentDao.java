package com.mycompany.app.db.controllers;

import com.mycompany.app.db.Dao;
import com.mycompany.app.db.models.ContentModel;

import java.sql.Connection;
import java.util.Collection;
import java.util.Optional;
import java.util.logging.Logger;

public class ContentDao implements Dao<ContentModel, Integer> {
    private static final Logger LOGGER = Logger.getLogger(CommitDao.class.getName());

    private final Optional<Connection> connection;

    public ContentDao(Optional<Connection> connection) throws ClassNotFoundException {
        this.connection =  connection;
    }


    @Override
    public Optional<ContentModel> get(int id) {
        return Optional.empty();
    }

    @Override
    public Collection<ContentModel> getAll() {
        return null;
    }

    @Override
    public Optional<Integer> save(ContentModel contentModel) {
        return Optional.empty();
    }

    @Override
    public void update(ContentModel contentModel) {

    }

    @Override
    public void delete(ContentModel contentModel) {

    }
}
