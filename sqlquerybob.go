package sqlquerybob

import (
	"fmt"
	"strings"
)

// A custom type that describes the database engine the query builder will build queries for
type database int8

// Supported database engines
const (
	MYSQL database = iota
	SQLITE
	POSTGRES
	ORACLE
)

// A custom type that describes the supported query types
type queryType int8

// Supported query types
const (
	selectQry queryType = iota
	insertQry
	updateQry
	deleteQry
)

// A custom type that describes the sort order of a query with ORDER BY
type sortOrder int8

// Supported sort order types
const (
	ascending sortOrder = iota
	descending
)

// Valid operators
const (
	validOperators = "=/>/</>=/<=/<>/IN/BETWEEN/LIKE"
)

type Builder struct {
	db               database
	placeholderCount int
	queryType        queryType
	table            string
	joinTables       []struct {
		joinType string
		table    string
		column   string
		fkey     string
	}
	columns          []string
	returningColumns []string
	values           []interface{}
	returnValues     []interface{}
	criteria         []struct {
		column   string
		operator string
		values   []interface{}
		or       bool
	}
	orderBy []struct {
		column    string
		direction sortOrder
	}
	limit  uint
	offset uint
}

// Creates a new query builder for SELECT. The table on which we are going
// to perform the select query is passed as the tableName parameter.
func NewSelect(tableName string) *Builder {
	return &Builder{
		queryType: selectQry,
		table:     tableName,
	}
}

// Sets the database engine the queries will be produced for
func (qb *Builder) ForDatabase(db database) *Builder {
	qb.db = db
	return qb
}

// Sets the database engine for MySQL
func (qb *Builder) ForMySQL() *Builder {
	return qb.ForDatabase(MYSQL)
}

// Sets the database engine for PostreSQL
func (qb *Builder) ForPostgres() *Builder {
	return qb.ForDatabase(POSTGRES)
}

// Sets the database engine for Oracle
func (qb *Builder) ForOracle() *Builder {
	return qb.ForDatabase(ORACLE)
}

// Sets the database engine for SQLite
func (qb *Builder) ForSQLite() *Builder {
	return qb.ForDatabase(SQLITE)
}

// Define the table columns to be selected. Table columns can be added by their name
// or prefixed by their table name. If a table name is not prefixed, the table that has
// been defined in NewSelect will be prefixed. For example
//   - NewSelect("table1").Select("column1", "table2.column5") will store the columns as
//     table1.column1, table2.column5
func (qb *Builder) Select(columns ...string) *Builder {
	for _, column := range columns {
		tableColumn := strings.Split(column, ".")
		if len(tableColumn) == 1 {
			qb.columns = append(qb.columns, qb.table+"."+column)
		}
		if len(tableColumn) == 2 {
			qb.columns = append(qb.columns, column)
		}
	}
	return qb
}

// Define the table columns to be returned from an insert.
func (qb *Builder) Returning(columns ...string) *Builder {
	for _, column := range columns {
		tableColumn := strings.Split(column, ".")
		if len(tableColumn) == 1 {
			qb.returningColumns = append(qb.returningColumns, qb.table+"."+column)
		}
		if len(tableColumn) == 2 {
			qb.returningColumns = append(qb.returningColumns, column)
		}
	}
	return qb
}

// Adds a limit and / or offset clause to the query. If offset is not required, pass 0 as the
// offset argument. Limit and offset must be non negative integers so we avoid this error by
// making they are uints.
func (qb *Builder) Limit(limit, offset uint) *Builder {
	qb.limit = limit
	qb.offset = offset
	return qb
}

func (qb *Builder) Set(columns ...string) *Builder {
	qb.columns = append(qb.columns, columns...)
	return qb
}

func (qb *Builder) To(values ...interface{}) *Builder {
	qb.values = append(qb.values, values...)
	return qb
}

// Define the values in which the query results will be stored. These have to be
// pointers.
func (qb *Builder) Into(values ...interface{}) *Builder {
	if qb.queryType == selectQry {
		qb.values = append(qb.values, values...)
	} else {
		qb.returnValues = append(qb.returnValues, values...)
	}

	return qb
}

// Define a join. Multiple joins can be added by chaining this.
func (qb *Builder) Join(joinType, table, column, fkey string) *Builder {
	qb.joinTables = append(
		qb.joinTables,
		struct {
			joinType, table, column, fkey string
		}{
			joinType, table, column, fkey,
		},
	)
	return qb
}

// Define the where clause of the query.
func (qb *Builder) Where(column, operator string, values ...interface{}) *Builder {
	qb.criteria = append(
		qb.criteria,
		struct {
			column   string
			operator string
			values   []interface{}
			or       bool
		}{
			column:   column,
			operator: strings.ToUpper(operator),
			values:   values,
			or:       false,
		},
	)
	return qb
}

// Define a where OR clause of the query.
func (qb *Builder) OrWhere(column, operator string, values ...interface{}) *Builder {
	qb.criteria = append(
		qb.criteria,
		struct {
			column   string
			operator string
			values   []interface{}
			or       bool
		}{
			column:   column,
			operator: strings.ToUpper(operator),
			values:   values,
			or:       true,
		},
	)
	return qb
}

// Define an ascending order on a column
func (qb *Builder) OrderBy(column string) *Builder {
	qb.orderBy = append(
		qb.orderBy,
		struct {
			column    string
			direction sortOrder
		}{
			column:    column,
			direction: ascending,
		},
	)
	return qb
}

// Define a descending order on a column
func (qb *Builder) OrderByDescending(column string) *Builder {
	qb.orderBy = append(
		qb.orderBy,
		struct {
			column    string
			direction sortOrder
		}{
			column:    column,
			direction: descending,
		},
	)
	return qb
}

// Returns the pointer values in which the results will be stored
func (qb *Builder) Values() []interface{} {
	return qb.values
}

// Returns the pointer values in which the returning values for a PostgreSQL or Oracle
// Insert, Update, Delete query with returning will be stored
func (qb *Builder) ReturningValues() []interface{} {
	return qb.returnValues
}

// Returns the criteria values that have been defined with Where
func (qb *Builder) Criteria() []interface{} {
	var values []interface{}
	for _, criterion := range qb.criteria {
		values = append(values, criterion.values...)
	}
	return values
}

// Generates the query string.
func (qb *Builder) GenerateQuery() (string, error) {
	var qry string
	var err error
	switch qb.queryType {
	case selectQry:
		qry, err = qb.generateSelectQry()
	case insertQry:
		qry, err = qb.generateInsertQry()
	case updateQry:
		qry, err = qb.generateUpdateQry()
	case deleteQry:
		qry, err = qb.generateDeleteQry()
	}
	return qry, err
}

func (qb *Builder) generateSelectQry() (string, error) {
	qry, err := qb.generateSelectClause()
	if err != nil {
		return "", err
	}
	qry += qb.generateFromAndJoinClause()
	whereClause, err := qb.generateWhereClause()
	if err != nil {
		return "", err
	}
	qry += whereClause
	qry += qb.generateOrderByClause()
	qry += qb.generateLimitClause()
	return qry, err
}

func (qb *Builder) generateDeleteQry() (string, error) {
	qry := qb.generateDeleteClause()
	qry += qb.generateFromAndJoinClause()
	whereClause, err := qb.generateWhereClause()
	if err != nil {
		return "", err
	}
	qry += whereClause
	returningClause, err := qb.generateReturningClause()
	if err != nil {
		return "", err
	}
	qry += returningClause
	return qry, err
}

func (qb *Builder) generateUpdateQry() (string, error) {
	qry, err := qb.generateUpdateClause()
	if err != nil {
		return "", err
	}
	whereClause, err := qb.generateWhereClause()
	if err != nil {
		return "", err
	}
	qry += whereClause
	returningClause, err := qb.generateReturningClause()
	if err != nil {
		return "", err
	}
	qry += returningClause
	return qry, err
}

func (qb *Builder) generateInsertQry() (string, error) {
	qry, err := qb.generateInsertClause()
	if err != nil {
		return "", err
	}
	returningClause, err := qb.generateReturningClause()
	if err != nil {
		return "", err
	}
	qry += returningClause
	return qry, nil
}

// Generates the SELECT clause. Will return error if the number of values is not equal
// to the number of columns
func (qb *Builder) generateSelectClause() (string, error) {
	if len(qb.columns) != len(qb.values) {
		return "", NewBadColumnsValuesComboError(len(qb.columns), len(qb.values))
	}
	qry := "SELECT "
	for i, column := range qb.columns {
		qry += column
		if i < len(qb.columns)-1 {
			qry += ","
		}
	}
	return qry, nil
}

// Generates the RETURNING clause. Will return error if
// a) the number of values is not equal to the number of returning columns
// b) the databse engine does not support the RETURNING clause (MySQL, SQLite)
func (qb *Builder) generateReturningClause() (string, error) {
	if len(qb.returningColumns) == 0 {
		return "", nil
	}
	if qb.db != POSTGRES && qb.db != ORACLE {
		return "", ErrDBEngineDoesNotSupportReturning
	}
	if len(qb.returningColumns) != len(qb.returnValues) {
		return "", NewBadColumnsValuesComboError(len(qb.returningColumns), len(qb.returnValues))
	}
	qry := " RETURNING "
	for i, column := range qb.returningColumns {
		qry += column
		if i < len(qb.returningColumns)-1 {
			qry += ","
		}
	}
	return qry, nil
}

// Generates the join clause
func (qb *Builder) generateFromAndJoinClause() string {
	qry := " FROM " + qb.table
	for _, joinTable := range qb.joinTables {
		qry += " " + joinTable.joinType +
			" JOIN " +
			joinTable.table +
			" ON " +
			joinTable.column +
			"=" +
			joinTable.fkey
	}
	return qry
}

// Generates the WHERE clause. Will return error if a comparison operator is invalid
func (qb *Builder) generateWhereClause() (string, error) {
	if len(qb.criteria) == 0 {
		return "", nil
	}
	qry := " WHERE "
	for ci, criterion := range qb.criteria {
		if ci == 0 && criterion.or {
			return "", ErrFirstCriterionIsOr
		}
		if ci != 0 && ci < len(qb.criteria) {
			switch criterion.or {
			case true:
				qry += " OR "
			default:
				qry += " AND "
			}
		}
		if !qb.operatorIsValid(criterion.operator) {
			return "", NewInvalidOperatorError(criterion.operator)
		}
		qry += criterion.column
		if criterion.operator == "BETWEEN" || criterion.operator == "IN" || criterion.operator == "LIKE" {
			qry += " "
		}
		qry += criterion.operator
		switch {
		case criterion.operator == "LIKE":
			qry += " " + qb.addPlaceholder()
		case criterion.operator == "BETWEEN":
			qry += " " + qb.addPlaceholder() + " AND " + qb.addPlaceholder()
		case criterion.operator == "IN":
			qry += " (" + qb.addPlaceholder() + strings.Repeat(","+qb.addPlaceholder(), len(criterion.values)-1) + ")"
		default:
			qry += qb.addPlaceholder()
		}
	}
	return qry, nil
}

// Generates the ORDER BY clause
func (qb *Builder) generateOrderByClause() string {
	if len(qb.orderBy) == 0 {
		return ""
	}
	qry := " ORDER BY "
	for ci, order := range qb.orderBy {
		qry += order.column
		switch {
		case order.direction == descending:
			qry += " DESC"
		default:
			qry += " ASC"
		}
		if ci < len(qb.orderBy)-1 {
			qry += ","
		}
	}
	return qry
}

func (qb *Builder) generateLimitClause() string {
	if qb.limit == 0 {
		return ""
	}
	qry := fmt.Sprintf(" LIMIT %d", qb.limit)
	if qb.offset > 0 {
		qry += fmt.Sprintf(",%d", qb.offset)
	}
	return qry
}

// Checks if a comparison operator is valid
func (qb *Builder) operatorIsValid(operator string) bool {
	for _, o := range strings.Split(validOperators, "/") {
		if operator == o {
			return true
		}
	}
	return false
}

func NewInsert(tableName string) *Builder {
	return &Builder{
		queryType: insertQry,
		table:     tableName,
	}
}

func NewUpdate(tableName string) *Builder {
	return &Builder{
		queryType: updateQry,
		table:     tableName,
	}
}

func NewDelete(tableName string) *Builder {
	return &Builder{
		queryType: deleteQry,
		table:     tableName,
	}
}

func (qb *Builder) generateInsertClause() (string, error) {
	if len(qb.columns) != len(qb.values) {
		return "", NewBadColumnsValuesComboError(len(qb.columns), len(qb.values))
	}
	qry := "INSERT INTO " + qb.table + " ("
	for i, column := range qb.columns {
		qry += column
		if i < len(qb.columns)-1 {
			qry += ","
		}
	}
	qry += ") VALUES ("
	for i := range qb.values {
		qry += qb.addPlaceholder()
		if i < len(qb.values)-1 {
			qry += ","
		}
	}
	qry += ")"
	return qry, nil
}

func (qb *Builder) generateUpdateClause() (string, error) {
	if len(qb.columns) != len(qb.values) {
		return "", NewBadColumnsValuesComboError(len(qb.columns), len(qb.values))
	}
	qry := "UPDATE " + qb.table + " SET "
	for i, column := range qb.columns {
		qry += column + "=" + qb.addPlaceholder()
		if i < len(qb.columns)-1 {
			qry += ","
		}
	}
	return qry, nil
}

func (qb *Builder) generateDeleteClause() string {
	qry := "DELETE"
	return qry
}

func (qb *Builder) addPlaceholder() string {
	qb.placeholderCount += 1
	switch qb.db {
	case POSTGRES:
		return fmt.Sprintf("$%d", qb.placeholderCount)
	case ORACLE:
		return fmt.Sprintf(":%d", qb.placeholderCount)
	default:
		return "?"
	}
}
