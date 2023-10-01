package sqlquerybob

import (
	"errors"
	"fmt"
)

// Errors
type ErrBadColumnsValuesCombo struct {
	columnCount int
	valueCount  int
	msg         string
}

func NewBadColumnsValuesComboError(columnCount, valueCount int) ErrBadColumnsValuesCombo {
	return ErrBadColumnsValuesCombo{
		columnCount: columnCount,
		valueCount:  valueCount,
		msg:         fmt.Sprintf("columns count (%d) must be equal to values count (%d)", columnCount, valueCount),
	}
}

func (e ErrBadColumnsValuesCombo) Error() string {
	return e.msg
}

type ErrInvalidSqlOperator struct {
	operator string
	msg      string
}

func NewInvalidOperatorError(operator string) ErrInvalidSqlOperator {
	return ErrInvalidSqlOperator{
		operator: operator,
		msg:      fmt.Sprintf("operator '%s' is an invalid SQL operator", operator),
	}
}

func (e ErrInvalidSqlOperator) Error() string {
	return e.msg
}

var ErrFirstCriterionIsOr = errors.New("the first criterion is an OR")

var ErrDBEngineDoesNotSupportReturning = errors.New("database engine does not support RETURNING clause")
