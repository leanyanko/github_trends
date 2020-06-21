package com.mycompany.app.db.models;

import java.io.Serializable;
import java.sql.Date;

public class LastCommitModel implements Serializable {
    private Integer id;
    private String repo;
    private Date date;

    public LastCommitModel(String repo, Date commit) {
        this.repo = repo;
        this.date = commit;
    }

    public LastCommitModel(String repo, java.util.Date commit) {
        this.repo = repo;
        this.date = new java.sql.Date(commit.getTime());
    }

    public LastCommitModel(int id, String repo, Date commit) {
        this.id = id;
        this.repo = repo;
        this.date = commit;
    }

    public Date getDate() {
        return date;
    }

    public Integer getId() {
        return id;
    }

    public String getRepo() {
        return repo;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setRepo(String repo) {
        this.repo = repo;
    }

    @Override
    public String toString() {
        return "LastC{" +
                "id=" + id +
                ", repo='" + repo + '\'' +
                ", date=" + date +
                '}';
    }
}
