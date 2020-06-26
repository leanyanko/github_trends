package com.mycompany.app.db.controllers;

import com.mycompany.app.db.Dao;
import com.mycompany.app.db.JDBCConnect;
import com.mycompany.app.db.models.CommitModel;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CommitDao implements Dao<CommitModel, Integer> {

    private static final Logger LOGGER = Logger.getLogger(CommitDao.class.getName());

    private final Optional<Connection> connection;

    public CommitDao(String credentials) throws ClassNotFoundException {
        String[] db_key = credentials.split("--");
        this.connection =  Optional.ofNullable(new JDBCConnect().getConnection(db_key[0], db_key[1], db_key[2]));
    }

    @Override
    public Optional<CommitModel> get(int id) {
        return connection.flatMap(conn -> {
            Optional<CommitModel> lc = Optional.empty();
            String sql = "SELECT * FROM all_commits WHERE id = " + id;

            try (Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {

                if (resultSet.next()) {
                    String repo = resultSet.getString("repo_name");
                    Date date = resultSet.getDate("date");

                    lc = Optional.of(
                            new CommitModel(id, repo, date));

                    LOGGER.log(Level.INFO, "Found {0} in database", lc.get());
                }
            } catch (SQLException ex) {
                LOGGER.log(Level.SEVERE, null, ex);
            }

            return lc;
        });
    }

    @Override
    public Collection<CommitModel> getAll() {
        System.out.println("GETTING ALL DATA FROM TABLE");
        final String sql = "SELECT * FROM all_commits";
        final Collection<CommitModel> exts = new ArrayList<>();
        connection.ifPresent(conn -> {
            try (Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {

                while (resultSet.next()) {
                    final int id = resultSet.getInt("id");
                    String repo = resultSet.getString("repo_name");
                    Date date = resultSet.getDate("date");

                    final CommitModel lc = new CommitModel(id, repo,date);

                    exts.add(lc);

                    LOGGER.log(Level.INFO, "Found {0} in database", lc);
                }

            } catch (final SQLException ex) {
                LOGGER.log(Level.SEVERE, null, ex);
            }
        });
        return exts;
    }

    @Override
    public Optional<Integer> save(CommitModel commitModel) {
        final String sql = "INSERT INTO all_commits_copy (repo_name, date) VALUES(?, ?)";
        String message = "The customer to be added should not be null";
        CommitModel nonNullExtention = Objects.requireNonNull(commitModel, message);

        return connection.flatMap(conn -> {
            Optional<Integer> generatedId = Optional.empty();

            try (PreparedStatement statement =
                         conn.prepareStatement(
                                 sql,
                                 Statement.RETURN_GENERATED_KEYS)) {

                statement.setString(1, nonNullExtention.getRepo());
                statement.setDate(2, nonNullExtention.getDate());
                int numberOfInsertedRows = statement.executeUpdate();

                // Retrieve the auto-generated id
                if (numberOfInsertedRows > 0) {
                    try (ResultSet resultSet = statement.getGeneratedKeys()) {
                        if (resultSet.next()) {
                            generatedId = Optional.of(resultSet.getInt(1));
                        }
                    }
                }
            } catch (SQLException ex) {
                LOGGER.log(Level.SEVERE, null, ex);
                System.out.println(ex);
            }

            return generatedId;
        });
    }

    @Override
    public void update(CommitModel lc) {
        String message = "The customer to be updated should not be null";
        CommitModel nonNullCustomer = Objects.requireNonNull(lc, message);
        String sql = "UPDATE last_commit "
                + "SET "
                + "repo_name = ?, "
                + "date = ? "
                + "WHERE "
                + "id = ?";

        connection.ifPresent(conn -> {
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                statement.setString(1, nonNullCustomer.getRepo());
                statement.setDate(2, nonNullCustomer.getDate());
                statement.setInt(3, nonNullCustomer.getId());

                int numberOfUpdatedRows = statement.executeUpdate();

                LOGGER.log(Level.INFO, "Was the customer updated successfully? {0}",
                        numberOfUpdatedRows > 0);

            } catch (SQLException ex) {
                LOGGER.log(Level.SEVERE, null, ex);
            }
        });
    }

    @Override
    public void delete(CommitModel lc) {
        String message = "The customer to be deleted should not be null";
        CommitModel nonNullCustomer = Objects.requireNonNull(lc, message);
        String sql = "DELETE FROM last_commit WHERE id = ?";

        connection.ifPresent(conn -> {
            try (PreparedStatement statement = conn.prepareStatement(sql)) {

                statement.setInt(1, nonNullCustomer.getId());

                int numberOfDeletedRows = statement.executeUpdate();

                LOGGER.log(Level.INFO, "Was the customer deleted successfully? {0}",
                        numberOfDeletedRows > 0);

            } catch (SQLException ex) {
                LOGGER.log(Level.SEVERE, null, ex);
            }
        });
    }
}
