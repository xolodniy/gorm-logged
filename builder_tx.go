package builder

import (
	"database/sql"
	"errors"

	"gorm-logged/common"

	"github.com/sirupsen/logrus"
)

// TransactionBuilder Interface for orchestrating transactions outside of model tier
type TransactionBuilder interface {
	Begin() *Model
	Commit() error
	RollbackWithError(err error) error
	RollBack()
}

// Begin initiate model layer as single transaction, you need to commit your changes at the end
func (m *Model) Begin() *Model {
	return &Model{db: m.db.Begin()}
}

// Commit stories changes of transaction
func (m *Model) Commit() error {
	if err := m.db.Commit().Error; err != nil {
		logrus.WithError(err).Error("can't commit transaction")
		return common.ErrInternal
	}
	return nil
}

// RollbackWithError skips changes from transaction exempts connection
func (m *Model) RollbackWithError(err error) error {
	if err := m.db.Rollback().Error; err != nil {
		logrus.WithError(err).Error("can't rollback transaction")
	}
	return err
}

// RollBack skips changes from transaction exempts connection
func (m *Model) RollBack() {
	err := m.db.Rollback().Error
	if err == nil {
		return
	}
	if errors.Is(err, sql.ErrTxDone) {
		return
	}
	logrus.WithError(err).Error("can't rollback transaction")
}
