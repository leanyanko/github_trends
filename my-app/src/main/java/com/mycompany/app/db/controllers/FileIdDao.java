package com.mycompany.app.db.controllers;

import com.mycompany.app.db.Dao;
import com.mycompany.app.db.JDBCConnect;
import com.mycompany.app.db.models.FileIdModel;
import com.mycompany.app.db.models.LangModel;

import java.sql.*;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileIdDao implements Dao <FileIdModel, Integer> {
    private static final Logger LOGGER = Logger.getLogger(CommitDao.class.getName());

    private final Optional<Connection> connection;

    public FileIdDao(String credentials) throws ClassNotFoundException {
        String[] db_key = credentials.split("--");
        this.connection =  Optional.ofNullable(new JDBCConnect().getConnection(db_key[0], db_key[1], db_key[2]));
    }

    @Override
    public Optional<FileIdModel> get(int id) {
        return Optional.empty();
    }

    @Override
    public Collection<FileIdModel> getAll() {
        return null;
    }

    @Override
    public Optional<Integer> save(FileIdModel fileIdModel) {
        final String sql = "INSERT INTO fileids (repo_name, fileid) VALUES(?, ?)";
        String message = "The customer to be added should not be null";
        FileIdModel nonNullExtention = Objects.requireNonNull(fileIdModel, message);

        return connection.flatMap(conn -> {
            Optional<Integer> generatedId = Optional.empty();

            try (PreparedStatement statement =
                         conn.prepareStatement(
                                 sql,
                                 Statement.RETURN_GENERATED_KEYS)) {

                statement.setString(1, nonNullExtention.getRepo_name());
                statement.setString(2, nonNullExtention.getFileId());
                int numberOfInsertedRows = statement.executeUpdate();

                // Retrieve the auto-generated id
                if (numberOfInsertedRows > 0) {
                    try (ResultSet resultSet = statement.getGeneratedKeys()) {
                        if (resultSet.next()) {
                            generatedId = Optional.of(resultSet.getInt(1));
                        }
                    }
                }
                // Too much of info
                LOGGER.log(
                        Level.INFO,
                        "{0} created successfully? {1}",
                        new Object[]{nonNullExtention,
                                (numberOfInsertedRows > 0)});
            } catch (SQLException ex) {
                LOGGER.log(Level.SEVERE, null, ex);
                System.out.println(ex);
            }

            return generatedId;
        });
    }

    @Override
    public void update(FileIdModel fileIdModel) {

    }

    @Override
    public void delete(FileIdModel fileIdModel) {

    }
}
