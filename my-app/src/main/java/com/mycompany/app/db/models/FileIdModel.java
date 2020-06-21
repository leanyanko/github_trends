package com.mycompany.app.db.models;

public class FileIdModel {
    private Integer id;
    private String repo_name;
    private String fileId;

    public FileIdModel(int id, String repo_name, String fileId) {
        this.id = id;
        this.repo_name = repo_name;
        this.fileId = fileId;
    }

    public FileIdModel(String repo_name, String fileId) {
        this.repo_name = repo_name;
        this.fileId = fileId;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getFileId() {
        return fileId;
    }

    public String getRepo_name() {
        return repo_name;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public void setRepo_name(String repo_name) {
        this.repo_name = repo_name;
    }


    @Override
    public String toString() {
        return "FileIdModel{" +
                "id=" + id +
                ", repo_name='" + repo_name + '\'' +
                ", fileId='" + fileId + '\'' +
                '}';
    }
}
