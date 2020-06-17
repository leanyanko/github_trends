const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const port = process.env.PORT || 3000;

app.use(bodyParser.json());
app.use(
  bodyParser.urlencoded({
    extended: true,
  })
);

const db = require('./queries');
app.get('/langs', db.getLangs);
app.get('/langs/:lang', db.getSpecificLang);
app.get('/lan/:name', db.getLangRepo);

app.get('/repoes/:day', db.getRepoByDay);
app.get('/repo-name/:name', db.getRepoByName);
app.get('repo/:name', db.getRepoByNameTmp);

app.get('/', (request, response) => {
    response.json({ info: 'Node.js, Express, and Postgres API' })
  });

  app.listen(port, () => {
    console.log(`App running on port ${port}.`)
  });