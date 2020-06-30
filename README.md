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
![Illustration2](https://github.com/meinou/github_trends/screenshots/commits_to_db.png)

### First stage: reprocessing of all commits to temporary files on S3
![Illustration3](https://github.com/meinou/github_trends/screenshots/commits_to_file.png)

### Initial app (not used anymore), but contains all necessary DAO's and parsers for possible additional processing if needed
![Illustration4](https://github.com/meinou/github_trends/screenshots/full.png)

### Node.js application with API for db if to use custom frontend
├── postgres-api

### Second stage: calculation, aggregation, creating CSV files for tableau
![Illustration5](https://github.com/meinou/github_trends/screenshots/process_commits.png)

### Frontend
└── visual