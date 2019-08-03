package dbutil

// GetIDList - Given a query that selects ids, return an array of those ids
import (
	"database/sql"
	"fmt"

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

func GetIDList(tx *sql.Tx, query string, args []interface{}) ([]int, error) {
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
