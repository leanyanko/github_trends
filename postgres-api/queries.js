const config = require('./config');
const Pool = require('pg').Pool;
const pool = new Pool(config);


const getLangs = (request, response) => {
    pool.query('SELECT * FROM langs', (error, results) => {
      if (error) {
        throw error
      }
      response.status(200).json(results.rows)
    })
  }

const getSpecificLang = (request, response) => {
      const lang = request.params.lang;
        pool.query('SELECT * FROM langs where lang = $1', [lang], (error, results) => {
          if (error) {
              console.log(lang);
            throw error
          }
          response.status(200).json(results.rows)
        })
    }

const getLangRepo = (request, response) => {
        const name = request.params.name.replace("--", "/");
          pool.query('SELECT * FROM langs where repo_name = $1', [name], (error, results) => {
            if (error) {
                console.log(lang);
              throw error
            }
            response.status(200).json(results.rows)
          })
      }


const getRepoByDay = (request, response) => {
        const day = request.params.day;
          pool.query('SELECT * FROM last_commit where date = $1', [day], (error, results) => {
            if (error) {
                console.log(date);
              throw error
            }
            response.status(200).json(results.rows)
          })
    }

    const getRepoByName = (request, response) => {
        const name =request.params.name;//.replace("--", "/") + '"';
        
          pool.query('SELECT * FROM last_commit where repo_name = $1', [name], (error, results) => {
            if (error) {
                console.log(name);
              throw error
            }
            response.status(200).json(results.rows)
          })
    }

    const getRepoByNameTmp = (request, response) => {
        const name = request.params.name.replace("--", "/");
        
          pool.query('SELECT * FROM last_commit where repo_name = $1', [name], (error, results) => {
            if (error) {
                console.log(name);
              throw error
            }
            response.status(200).json(results.rows)
          })
    }

  

module.exports = {
    getLangs,
    getSpecificLang,
    getRepoByDay,
    getRepoByName,
    getRepoByNameTmp,
    getLangRepo
}