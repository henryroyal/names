package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

var (
	dbUser string = os.Getenv("DATABASE_USER")
	dbPass string = os.Getenv("DATABASE_PASS")
	dbHost string = os.Getenv("DATABASE_HOST")
	dbPort string = os.Getenv("DATABASE_PORT")
	dbName string = os.Getenv("DATABASE_NAME")
	dbSSL  string = os.Getenv("DATABASE_SSL")

	firstnamesUrl string = "https://raw.githubusercontent.com/philipperemy/name-dataset/master/names_dataset/first_names.all.txt"
	lastnamesUrl  string = "https://raw.githubusercontent.com/philipperemy/name-dataset/master/names_dataset/last_names.all.txt"

	tableExistsQuery     string = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_catalog = $1 AND table_name = $2);"
	insertFirstNameQuery string = "INSERT INTO first_names(name) VALUES ($1) ON CONFLICT DO NOTHING"
	insertLastNameQuery  string = "INSERT INTO last_names(name) VALUES ($1) ON CONFLICT DO NOTHING"
	createTablesQuery    string = `
		CREATE TABLE first_names (
			id   SERIAL,
			name varchar(100) UNIQUE NOT NULL
		);
		
		CREATE INDEX first_names_ix
			on first_names (name);
		
		CREATE TABLE last_names (
			id   SERIAL,
			name varchar(100) UNIQUE NOT NULL
		);
		
		CREATE INDEX last_names_ix
			on last_names (name);
	`
)

func makeDatabaseUrl(user, pass, host, port, database, ssl string) string {
	log.Printf("database: postgres://%v:******@%v:%v/%v?sslmode=%v", user, host, port, database, ssl)
	url := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=%v", user, pass, host, port, database, ssl)
	return url
}

func download(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func split(body string) []string {
	return strings.Split(body, "\n")
}

func tableExists(catalog string, table string, db *sql.DB) (exists bool) {
	rows, err := db.Query(tableExistsQuery, catalog, table)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	rows.Next()
	if err := rows.Scan(&exists); err != nil {
		log.Fatal(err)
	}

	return exists
}

func insert(names []string, db sql.DB) error {
	return nil
}

func main() {
	databaseUrl := makeDatabaseUrl(dbUser, dbPass, dbHost, dbPort, dbName, dbSSL)

	db, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Ping(); err == nil {
		log.Println("database: connect OK")
	}

	rawFirstNames, err := download(firstnamesUrl)
	if err != nil {
		log.Fatal(err)
	}
	firstNames := split(rawFirstNames)
	log.Printf("download: %v first names", len(firstNames))

	rawLastNames, err := download(lastnamesUrl)
	if err != nil {
		log.Fatal(err)
	}
	lastNames := split(rawLastNames)
	log.Printf("download: %v last names", len(lastNames))

	if !tableExists(dbName, "first_names", db) && !tableExists(dbName, "last_names", db) {
		cursor, err := db.Query(createTablesQuery)
		if err != nil {
			log.Fatal(err)
		}
		defer cursor.Close()
		log.Println("database: created name tables")
	} else {
		log.Println("database: found name tables")
	}

	go func() {
		log.Println("database: inserting first names")
		for _, v := range firstNames {
			cursor, err := db.Query(insertFirstNameQuery, v)
			if err != nil {
				log.Fatal(err)
			}
			cursor.Close()
		}
		log.Println("database: first names complete")
	}()

	log.Println("database: inserting last names")
	for _, v := range lastNames {
		cursor, err := db.Query(insertLastNameQuery, v)
		if err != nil {
			log.Fatal(err)
		}
		cursor.Close()
	}
	log.Println("database: last names complete")

}
