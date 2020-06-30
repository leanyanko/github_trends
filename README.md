# GitSee by Anna Leonenko

## InsightDataScience Data Engineering project

## What technology is trending on a github? What language is popular? For how long?

# [Demo: datathing.xyz](https://datathing.xyz)

# Let's explore. Detailed filesystem is below the illustrations.

![Illustration](https://github.com/meinou/github_trends/screenshots/illustration)

## To start: here is a structure of the repo, it consists of multiple maven projects which compile to separate jars for each application stage

├── README.md

### screenshots for readme

├── screenshots 

### third stage - already processes commits to DataBase

├── commits_to_db  
│   ├── example.txt

│   ├── pom.xml

│   └── src

│       ├── main

│       │   ├── java

│       │   │   └── com
│       │   │       └── mycompany
│       │   │           └── app
│       │   │               ├── App.java
│       │   │               ├── db
│       │   │               │   ├── Dao.java
│       │   │               │   ├── JDBCConnect.java
│       │   │               │   ├── controllers
│       │   │               │   │   └── CommitDao.java
│       │   │               │   ├── db_init.sql
│       │   │               │   └── models
│       │   │               │       ├── LangModel.java
│       │   │               │       └── LastCommitModel.java
│       │   │               └── processors
│       │   │                   └── LastCommitProcessor.java
│       │   ├── main.iml
│       │   └── resources
│       │       └── spark_example.txt
│       └── test
│           ├── java
│           │   └── com
│           │       └── mycompany
│           │           └── app
│           │               ├── AppTest.java
│           │               └── TransformerTest.java
│           └── test.iml

### First stage: reprocessing of all commits to temporary files on S3
├── commits_to_file
│   ├── example.txt
│   ├── pom.xml
│   └── src
│       ├── main
│       │   ├── java
│       │   │   └── com
│       │   │       └── mycompany
│       │   │           └── app
│       │   │               ├── App.java
│       │   │               └── processors
│       │   │                   └── LastCommitProcessor.java
│       │   ├── main.iml
│       │   └── resources
│       │       └── spark_example.txt
│       └── test
│           ├── java
│           │   └── com
│           │       └── mycompany
│           │           └── app
│           │               ├── AppTest.java
│           │               └── TransformerTest.java
│           └── test.iml
### Initial app (not used anymore), but contains all necessary DAO's and parsers for possible additional processing if needed
├── my-app
│   ├── example.txt
│   ├── pom.xml
│   └── src
│       ├── main
│       │   ├── java
│       │   │   └── com
│       │   │       └── mycompany
│       │   │           └── app
│       │   │               ├── App.java
│       │   │               ├── db
│       │   │               │   ├── Dao.java
│       │   │               │   ├── JDBCConnect.java
│       │   │               │   ├── controllers
│       │   │               │   │   ├── CommitDao.java
│       │   │               │   │   ├── ContentDao.java
│       │   │               │   │   ├── FileIdDao.java
│       │   │               │   │   └── LanguageDao.java
│       │   │               │   ├── db_init.sql
│       │   │               │   └── models
│       │   │               │       ├── ContentModel.java
│       │   │               │       ├── FileIdModel.java
│       │   │               │       ├── LangModel.java
│       │   │               │       └── LastCommitModel.java
│       │   │               └── processors
│       │   │                   ├── FieldIdProcessor.java
│       │   │                   ├── LangProcessor.java
│       │   │                   └── LastCommitProcessor.java
│       │   ├── main.iml
│       │   └── resources
│       │       └── spark_example.txt
│       └── test
│           ├── java
│           │   └── com
│           │       └── mycompany
│           │           └── app
│           │               ├── AppTest.java
│           │               └── TransformerTest.java
│           └── test.iml
### Node.js application with API for db if to use custom frontend
├── postgres-api
│   ├── index.js
│   ├── l_ordered.txt
│   ├── langs.txt
│   ├── package-lock.json
│   ├── package.json
│   ├── queries.js
│   └── short_list.txt
### Second stage: calculation, aggregation, creating CSV files for tableau
├── process_commits
│   ├── example.txt
│   ├── pom.xml
│   └── src
│       ├── main
│       │   ├── java
│       │   │   └── com
│       │   │       └── mycompany
│       │   │           └── app
│       │   │               ├── App.java
│       │   │               ├── db
│       │   │               │   ├── Dao.java
│       │   │               │   ├── JDBCConnect.java
│       │   │               │   ├── controllers
│       │   │               │   │   └── CommitDao.java
│       │   │               │   ├── db_init.sql
│       │   │               │   └── models
│       │   │               │       ├── CommitModel.java
│       │   │               │       └── LangModel.java
│       │   │               └── processors
│       │   │                   └── CommitProcessor.java
│       │   ├── main.iml
│       │   └── resources
│       │       └── spark_example.txt
│       └── test
│           ├── java
│           │   └── com
│           │       └── mycompany
│           │           └── app
│           │               ├── AppTest.java
│           │               └── TransformerTest.java
│           └── test.iml
### Frontend
└── visual
    ├── README.md
    ├── package-lock.json
    ├── package.json
    ├── public
    │   ├── favicon.ico
    │   ├── index.html
    │   ├── logo192.png
    │   ├── logo512.png
    │   ├── manifest.json
    │   └── robots.txt
    └── src
        ├── App.css
        ├── App.js
        ├── App.test.js
        ├── index.css
        ├── index.js
        ├── logo.svg
        ├── serviceWorker.js
        └── setupTests.js