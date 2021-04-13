package dbutil

// GetIDList - Given a query that selects ids, return an array of those ids
import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

func Connect(dbName string, dbUser string, dbPassword string, dbHost string, dbPort int) (*sql.DB, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, errors.Wrap(err, "error opening connecting to database")
	}

	err = db.Ping()
	if err != nil {
		return nil, errors.Wrap(err, "error pinging database")
	}

	return db, nil
}

func GetIDList(tx *sql.Tx, query string, args ...interface{}) ([]int, error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "query failed")
	}

	var ids []int
	for rows.Next() {
		var id int

		err := rows.Scan(&id)
		if err != nil {
			rows.Close()
			return nil, errors.Wrap(err, "error scanning row")
		}

		ids = append(ids, id)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating over rows")
	}

	return ids, nil
}

func GetID(tx *sql.Tx, query string, args ...interface{}) (int, error) {
	var id int
	row := tx.QueryRow(query, args...)
	err := row.Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, err
		}

		return 0, errors.Wrap(err, "unable to get id")
	}

	return id, nil
}

func GetUUIDList(tx *sql.Tx, query string, args ...interface{}) ([]string, error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "query failed")
	}

	var uuids []string
	for rows.Next() {
		var uuid string

		err := rows.Scan(&uuid)
		if err != nil {
			rows.Close()
			return nil, errors.Wrap(err, "error scanning row")
		}

		uuids = append(uuids, uuid)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating over rows")
	}

	return uuids, nil
}
func GetUUID(tx *sql.Tx, query string, args ...interface{}) (string, error) {
	var uuid string
	row := tx.QueryRow(query, args...)
	err := row.Scan(&uuid)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", err
		}

		return "", errors.Wrap(err, "unable to get uuid")
	}

	return uuid, nil
}

func FormatQuery(query string) string {
	numParams := strings.Count(query, "?")

	for i := 1; i <= numParams; i++ {
		query = strings.Replace(query, "?", "$"+strconv.Itoa(i), 1)
	}

	return query
}
