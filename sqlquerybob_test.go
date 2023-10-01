package sqlquerybob

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestItCreatesASimpleSQLStatementWithNoCriteria(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4").
		Into(&d.field1, &d.field2, &d.field3, &d.field4)
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	assert.Nil(err)
	expected := "SELECT table1.field1,table1.field2,table1.field3,table1.field4" +
		" FROM table1"
	assert.Equal(expected, qry)
	assert.Equal([]any{&d.field1, &d.field2, &d.field3, &d.field4}, qb.Values())
}

func TestItReturnsErrorIfColumnsCountNotEqualToValuesCount(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	qb := NewSelect("table1").
		Select("field1", "field2", "field3").
		Into(&d.field1, &d.field2, &d.field3, &d.field4)
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	assert.ErrorIs(err, err.(ErrBadColumnsValuesCombo))
	assert.NotNil(err)
	assert.Equal("", qry)
}

func TestItCreatesASimpleSQLStatementWithOrder(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4").
		Into(&d.field1, &d.field2, &d.field3, &d.field4).
		OrderBy("table1.field1").
		OrderByDescending("table1.field2")
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "SELECT table1.field1,table1.field2,table1.field3,table1.field4" +
		" FROM table1" +
		" ORDER BY table1.field1 ASC,table1.field2 DESC"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{&d.field1, &d.field2, &d.field3, &d.field4}, qb.Values())
}

func TestItCreatesASimpleSQLStatement(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4").
		Into(&d.field1, &d.field2, &d.field3, &d.field4).
		Where("table1.field1", "=", "value1")
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "SELECT table1.field1,table1.field2,table1.field3,table1.field4" +
		" FROM table1" +
		" WHERE table1.field1=?"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{&d.field1, &d.field2, &d.field3, &d.field4}, qb.Values())
	assert.Equal([]any{"value1"}, qb.Criteria())
}

func TestItCreatesASimpleSQLStatementWithLimit(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4").
		Into(&d.field1, &d.field2, &d.field3, &d.field4).
		Where("table1.field1", "=", "value1").
		Limit(10, 0)
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "SELECT table1.field1,table1.field2,table1.field3,table1.field4" +
		" FROM table1" +
		" WHERE table1.field1=?" +
		" LIMIT 10"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{&d.field1, &d.field2, &d.field3, &d.field4}, qb.Values())
	assert.Equal([]any{"value1"}, qb.Criteria())
}

func TestItCreatesASimpleSQLStatementWithLimitAndOffset(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4").
		Into(&d.field1, &d.field2, &d.field3, &d.field4).
		Where("table1.field1", "=", "value1").
		Limit(10, 50)
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "SELECT table1.field1,table1.field2,table1.field3,table1.field4" +
		" FROM table1" +
		" WHERE table1.field1=?" +
		" LIMIT 10,50"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{&d.field1, &d.field2, &d.field3, &d.field4}, qb.Values())
	assert.Equal([]any{"value1"}, qb.Criteria())
}

func TestItCreatesASimpleSQLStatementWithAND(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4").
		Into(&d.field1, &d.field2, &d.field3, &d.field4).
		Where("table1.field1", "=", "value1").
		Where("table1.field2", "=", "value2")
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "SELECT table1.field1,table1.field2,table1.field3,table1.field4" +
		" FROM table1" +
		" WHERE table1.field1=?" +
		" AND table1.field2=?"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{&d.field1, &d.field2, &d.field3, &d.field4}, qb.Values())
	assert.Equal([]any{"value1", "value2"}, qb.Criteria())
}

func TestItCreatesASimpleSQLStatementWithOR(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4").
		Into(&d.field1, &d.field2, &d.field3, &d.field4).
		Where("table1.field1", "=", "value1").
		OrWhere("table1.field2", "=", "value2")
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "SELECT table1.field1,table1.field2,table1.field3,table1.field4" +
		" FROM table1" +
		" WHERE table1.field1=?" +
		" OR table1.field2=?"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{&d.field1, &d.field2, &d.field3, &d.field4}, qb.Values())
	assert.Equal([]any{"value1", "value2"}, qb.Criteria())
}

func TestItReturnsAnErrorIfAWhereOperatorIsInvalid(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4").
		Into(&d.field1, &d.field2, &d.field3, &d.field4).
		Where("table1.field1", "=", "value1").
		Where("table1.field2", "ins", 1, 2, 3)
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	assert.ErrorIs(err, err.(ErrInvalidSqlOperator))
	assert.Equal("", qry)
}

func TestItReturnsAnErrorIfTheFirstCriterionIsAnOR(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4").
		Into(&d.field1, &d.field2, &d.field3, &d.field4).
		OrWhere("table1.field1", "=", "value1")
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	assert.ErrorIs(err, ErrFirstCriterionIsOr)
	assert.Equal("", qry)
}

func TestItCreatesAnSQLStatementWithINoperator(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	criteria := []any{1, 2, 3}
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4").
		Into(&d.field1, &d.field2, &d.field3, &d.field4).
		Where("table1.field2", "in", criteria...)
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "SELECT table1.field1,table1.field2,table1.field3,table1.field4" +
		" FROM table1" +
		" WHERE table1.field2 IN (?,?,?)"
	assert.Nil(err)
	assert.Equal(expected, qry)
}

func TestItCreatesAnSQLStatementWithLIKEoperator(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
	}
	var d dataStruct
	criteria := []any{"test"}
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4").
		Into(&d.field1, &d.field2, &d.field3, &d.field4).
		Where("table1.field2", "like", criteria...)
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "SELECT table1.field1,table1.field2,table1.field3,table1.field4" +
		" FROM table1" +
		" WHERE table1.field2 LIKE ?"
	assert.Nil(err)
	assert.Equal(expected, qry)
}

func TestItCreatesAnSQLStatementWithJoins(t *testing.T) {
	type dataStruct struct {
		field1 string
		field2 int
		field3 int
		field4 string
		field5 string
		field6 string
	}
	var d dataStruct
	qb := NewSelect("table1").
		Select("field1", "field2", "field3", "field4", "table2.field5", "table3.field6").
		Into(&d.field1, &d.field2, &d.field3, &d.field4, &d.field5, &d.field6).
		Join("LEFT", "table2", "table2.table1_id", "table1.id").
		Join("LEFT", "table3", "table3.table1_id", "table1.id").
		Where("table1.field1", "=", "value1").
		Where("table1.field2", "IN", 1, 2, 3, 4).
		Where("table1.field3", "BETWEEN", 1, 10)

	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "SELECT table1.field1,table1.field2,table1.field3,table1.field4,table2.field5,table3.field6" +
		" FROM table1 LEFT JOIN table2 ON table2.table1_id=table1.id" +
		" LEFT JOIN table3 ON table3.table1_id=table1.id" +
		" WHERE table1.field1=? AND table1.field2 IN (?,?,?,?) AND table1.field3 BETWEEN ? AND ?"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{&d.field1, &d.field2, &d.field3, &d.field4, &d.field5, &d.field6}, qb.Values())
	assert.Equal([]any{"value1", 1, 2, 3, 4, 1, 10}, qb.Criteria())
}

func TestItCreatesASimpleDeleteStatement(t *testing.T) {
	qb := NewDelete("table1").Where("table1.field1", "=", "value1")
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "DELETE FROM table1 WHERE table1.field1=?"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{"value1"}, qb.Criteria())
}

func TestItReturnsAnErrorIfDeleteWhereClauseInvalid(t *testing.T) {
	qb := NewDelete("table1").Where("table1.field1", "!=", "value1")
	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	assert.NotNil(err)
	assert.Equal("", qry)
}

func TestItCreatesASimpleUpdateStatement(t *testing.T) {
	qb := NewUpdate("table1").
		Set("field1", "field2", "field3", "field4").
		To("value1", 2, 5, "value4").
		Where("table1.id", "=", 10)

	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "UPDATE table1 SET field1=?,field2=?,field3=?,field4=?" +
		" WHERE table1.id=?"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{"value1", 2, 5, "value4"}, qb.Values())
	assert.Equal([]any{10}, qb.Criteria())
}

func TestItReturnsAnErrorIfAnUpdateWhereClauseIsInvalid(t *testing.T) {
	qb := NewUpdate("table1").
		Set("field1", "field2", "field3", "field4").
		To("value1", 2, 5, "value4").
		Where("table1.id", "!=", 10)

	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	assert.NotNil(err)
	assert.Equal("", qry)
}

func TestItReturnsAnErrorIfUpdateColumnsNotEqualToValues(t *testing.T) {
	qb := NewUpdate("table1").
		Set("field1", "field2", "field3", "field4").
		To("value1", 2, 5).
		Where("table1.id", "=", 10)

	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	assert.NotNil(err)
	assert.Equal("", qry)
}

func TestItCreatesASimpleInsertStatement(t *testing.T) {
	qb := NewInsert("table1").
		Set("field1", "field2", "field3", "field4").
		To("value1", 2, 5, "value4")

	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "INSERT INTO table1 (field1,field2,field3,field4) VALUES (?,?,?,?)"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{"value1", 2, 5, "value4"}, qb.Values())
}

func TestItCreatesASimpleInsertStatementForPostgres(t *testing.T) {
	qb := NewInsert("table1").
		ForDatabase(POSTGRES).
		Set("field1", "field2", "field3", "field4").
		To("value1", 2, 5, "value4")

	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "INSERT INTO table1 (field1,field2,field3,field4) VALUES ($1,$2,$3,$4)"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{"value1", 2, 5, "value4"}, qb.Values())
}

func TestItCreatesAnInsertStatementForPostgresWithReturningClause(t *testing.T) {
	var d struct {
		id     int
		field1 string
	}
	qb := NewInsert("table1").
		ForDatabase(POSTGRES).
		Set("field1", "field2", "field3", "field4").
		To("value1", 2, 5, "value4").
		Returning("id", "field1").
		Into(&d.id, &d.field1)

	qry, err := qb.GenerateQuery()
	assert := assert.New(t)
	expected := "INSERT INTO table1 (field1,field2,field3,field4) VALUES ($1,$2,$3,$4)" +
		" RETURNING table1.id,table1.field1"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{"value1", 2, 5, "value4"}, qb.Values())
	assert.Equal([]any{&d.id, &d.field1}, qb.ReturningValues())
}

func TestItCreatesASimpleInsertStatementForOracle(t *testing.T) {
	qb := NewInsert("table1").
		ForDatabase(ORACLE).
		Set("field1", "field2", "field3", "field4").
		To("value1", 2, 5, "value4")

	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	expected := "INSERT INTO table1 (field1,field2,field3,field4) VALUES (:1,:2,:3,:4)"
	assert.Nil(err)
	assert.Equal(expected, qry)
	assert.Equal([]any{"value1", 2, 5, "value4"}, qb.Values())
}

func TestItReturnsAnErrorIfInsertColumnsNotEqualToValues(t *testing.T) {
	qb := NewInsert("table1").
		Set("field1", "field2", "field3", "field4").
		To("value1", 2, 5)

	qry, err := qb.GenerateQuery()

	assert := assert.New(t)
	assert.NotNil(err)
	assert.Equal("", qry)
}
