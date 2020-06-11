package com.mycompany.app.db.models;

import java.sql.Date;

public class LastC {
    private Integer id;
    private String repo;
    private Date commit;

    public LastC(String repo, Date commit) {
        this.repo = repo;
        this.commit = commit;
    }

    public LastC(int id, String repo, Date commit) {
        this.id = id;
        this.repo = repo;
        this.commit = commit;
    }

    public Date getCommit() {
        return commit;
    }

    public Integer getId() {
        return id;
    }

    public String getRepo() {
        return repo;
    }

    public void setCommit(Date commit) {
        this.commit = commit;
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
                ", commit=" + commit +
                '}';
    }
}
