package com.mycompany.app.db.models;

public class LangModel {
    private Integer id;
    private String repo;
    private String lang;
    private Long bytes;

    public LangModel(int id, String repo, String lang, long bytes) {
        this.id = id;
        this.repo = repo;
        this.lang = lang;
        this.bytes = bytes;
    }

    public LangModel(String repo, String lang, long bytes) {
        this.repo = repo;
        this.lang = lang;
        this.bytes = bytes;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setRepo(String repo) {
        this.repo = repo;
    }

    public String getRepo() {
        return repo;
    }

    public Integer getId() {
        return id;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String language) {
        this.lang = lang;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public Long getBytes() {
        return bytes;
    }
}
