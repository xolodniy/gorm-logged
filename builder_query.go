package builder

import (
	"errors"
	"fmt"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"

	"gorm-logged/common"

	"github.com/sirupsen/logrus"
	"github.com/xolodniy/pretty"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Model struct {
	db *gorm.DB

	// support field for QueryBuilder interface
	// Used for tracing during building sql query.
	// Must be initialized separately for each query.
	logTrace logrus.Fields

	// store preload fields instead of instant preloading
	// allows to use builder outside model tier with a both preloading and counting
	preloads []struct {
		field      string
		conditions []interface{}
	}
}

func New(connURL string) Model {
	postgres.New(postgres.Config{}) // required for connect right driver
	db, err := gorm.Open(postgres.Open(connURL), &gorm.Config{
		Logger: logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
			logger.Config{
				SlowThreshold:             200 * time.Millisecond,
				LogLevel:                  logger.Warn,
				IgnoreRecordNotFoundError: true,
				Colorful:                  true,
			},
		),
	})
	if err != nil {
		logrus.WithError(err).Fatal("can't connect to database")
	}
	return Model{db: db}
}

// QueryBuilder expands default gorm methods
// there are embed logging, common errors and little bit more simply signature
type QueryBuilder interface {
	Preload(column string, conditions ...interface{}) *Model
	Debug() *Model
	Unscoped() *Model
	IgnoreConflicts() *Model
	Model(value interface{}) *Model
	Select(query interface{}, args ...interface{}) *Model
	Table(name string) *Model
	Limit(limit int) *Model
	Offset(offset int) *Model
	Order(value interface{}) *Model
	Set(name string, value interface{}) *Model
	Pluck(column string, value interface{}) error
	First(out interface{}, where ...interface{}) error
	Last(out interface{}, where ...interface{}) error
	Find(out interface{}, where ...interface{}) error
	Scan(dest interface{}) error
	Create(value interface{}) error
	Save(value interface{}) error
	Omit(value ...string) *Model
	Updates(attrs interface{}) error
	Delete(value interface{}, where ...interface{}) error
	Where(query interface{}, args ...interface{}) *Model
	Count() (int64, error)
	Not(query interface{}, args ...interface{}) *Model
	Group(name string) *Model
	Having(query interface{}, args ...interface{}) *Model
	Take(dest interface{}, conds ...interface{}) error
	BatchFind(dest interface{}, batchSize int, fc func(tx *Model, batch int) error) error
	Joins(query string, args ...interface{}) *Model
	UpdateByFilter(filter interface{}, values interface{}) error

	exec(sql string, values ...interface{}) error
}

func initLogTrace(trace logrus.Fields) logrus.Fields {
	if trace == nil {
		return make(logrus.Fields)
	}
	return trace
}

// Preload is gorm interface func
// ACHTUNG! do not edit if you don't sure how is pointers work here
func (m *Model) Preload(column string, conditions ...interface{}) *Model {
	trace := initLogTrace(m.logTrace)
	trace["preloadColumn-"+column] = column
	if len(conditions) > 0 {
		trace["preloadConditions-"+column] = conditions
	}
	return &Model{db: m.db, logTrace: trace, preloads: append(m.preloads, struct {
		field      string
		conditions []interface{}
	}{field: column, conditions: conditions})}
}

// recursive apply preloads
// we can't just apply preloads in cycle because we don't have dummy method for copy gorm db instance
// if we write query variable like next:
// query := m.db
// for i := range m.preloads { query = query.Preload(...) }
// we will apply preloads to the whole model, bkz query will be just a pointer to
func (m *Model) applyPreloads() *Model {
	if len(m.preloads) > 0 {
		m := &Model{
			db:       m.db.Preload(m.preloads[0].field, m.preloads[0].conditions...),
			logTrace: m.logTrace,
			preloads: m.preloads[1:]}
		return m.applyPreloads()
	}
	return &Model{db: m.db, logTrace: m.logTrace}
}

// Debug is gorm interface func
func (m *Model) Debug() *Model {
	return &Model{db: m.db.Debug(), logTrace: m.logTrace, preloads: m.preloads}
}

// Unscoped is gorm interface func
func (m *Model) Unscoped() *Model {
	trace := initLogTrace(m.logTrace)
	trace["unscoped"] = true
	return &Model{db: m.db.Unscoped(), logTrace: m.logTrace, preloads: m.preloads}
}

// Model is gorm interface func
func (m *Model) Model(value interface{}) *Model {
	trace := initLogTrace(m.logTrace)
	trace["model"] = pretty.Print(value)
	return &Model{db: m.db.Model(value), logTrace: trace, preloads: m.preloads}
}

// Select is gorm interface func
func (m *Model) Select(query interface{}, args ...interface{}) *Model {
	trace := initLogTrace(m.logTrace)
	trace["selectQuery"] = query
	if len(args) > 0 {
		trace["selectArgs"] = pretty.Print(args)
	}
	return &Model{db: m.db.Select(query, args...), logTrace: trace, preloads: m.preloads}
}

// Table is gorm interface func
func (m *Model) Table(name string) *Model {
	trace := initLogTrace(m.logTrace)
	trace["tableName"] = name
	return &Model{db: m.db.Table(name), logTrace: trace, preloads: m.preloads}
}

// Limit is gorm interface func
func (m *Model) Limit(limit int) *Model {
	trace := initLogTrace(m.logTrace)
	trace["limit"] = limit
	return &Model{db: m.db.Limit(limit), logTrace: trace, preloads: m.preloads}
}

// Offset is gorm interface func
func (m *Model) Offset(offset int) *Model {
	trace := initLogTrace(m.logTrace)
	trace["offset"] = offset
	return &Model{db: m.db.Offset(offset), logTrace: trace, preloads: m.preloads}
}

// Order is gorm interface func
func (m *Model) Order(value interface{}) *Model {
	trace := initLogTrace(m.logTrace)
	trace["order"] = pretty.Print(value)
	return &Model{db: m.db.Order(value), logTrace: trace, preloads: m.preloads}
}

// Joins is gorm interface func
func (m *Model) Joins(query string, args ...interface{}) *Model {
	trace := initLogTrace(m.logTrace)
	var i int
	for {
		if _, ok := trace["joinsQuery"+strconv.Itoa(i)]; !ok {
			break
		}
		i++
	}
	trace["joinsQuery"+strconv.Itoa(i)] = query
	if len(args) > 0 {
		trace["joinsArgs"+strconv.Itoa(i)] = pretty.Print(args)
	}
	return &Model{db: m.db.Joins(query, args), logTrace: trace, preloads: m.preloads}
}

func (m *Model) Set(name string, value interface{}) *Model {
	trace := initLogTrace(m.logTrace)
	var i int
	for {
		if _, ok := trace["setName"+strconv.Itoa(i)]; !ok {
			break
		}
		i++
	}
	trace["setName"+strconv.Itoa(i)] = name
	trace["setValue"+strconv.Itoa(i)] = value
	return &Model{db: m.db.Set(name, value), logTrace: trace, preloads: m.preloads}
}
func (m *Model) IgnoreConflicts() *Model {
	trace := initLogTrace(m.logTrace)
	trace["ignoreConflicts"] = true
	return &Model{db: m.db.Clauses(clause.OnConflict{DoNothing: true}), logTrace: trace, preloads: m.preloads}
}

// Pluck is gorm interface func
func (m *Model) Pluck(column string, value interface{}) error {
	err := m.applyPreloads().db.Pluck(column, value).Error
	if err != nil {
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logrus.Fields{
			"typeOfPluckingValue": fmt.Sprintf("%T", value),
			"pluckColumnName":     column,
			"trace":               common.GetFrames(),
		}).Error("can't pluck object from the database")
		return common.ErrInternal
	}
	return nil
}

// First is gorm interface func
func (m *Model) First(out interface{}, where ...interface{}) error {
	err := m.applyPreloads().db.First(out, where...).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return common.ErrNotFound
	}
	if err != nil {
		logFields := logrus.Fields{
			"trace":    common.GetFrames(),
			"firstOut": pretty.Print(out),
		}
		if len(where) > 0 {
			logFields["firstWhere"] = pretty.Print(where)
		}
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logFields).Error("can't get first object from the database")
		return common.ErrInternal
	}
	return nil
}

// Last is gorm interface func
func (m *Model) Last(out interface{}, where ...interface{}) error {
	err := m.applyPreloads().db.Last(out, where...).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return common.ErrNotFound
	}
	if err != nil {
		logFields := logrus.Fields{
			"trace":   common.GetFrames(),
			"lastOut": pretty.Print(out),
		}
		if len(where) > 0 {
			logFields["lastWhere"] = pretty.Print(where)
		}
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logFields).Error("can't get last object from the database")
		return common.ErrInternal
	}
	return nil
}

// Take is gorm interface func
func (m *Model) Take(dest interface{}, conds ...interface{}) error {
	err := m.applyPreloads().db.Take(dest, conds...).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return common.ErrNotFound
	}
	if err != nil {
		logFields := logrus.Fields{
			"takeWhereCondition": fmt.Sprintf("%+v", conds),
			"takeDest":           pretty.Print(dest),
			"trace":              common.GetFrames(),
		}
		if len(conds) > 0 {
			logFields["takeConds"] = pretty.Print(conds)
		}
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logFields).Error("can't take object from the database")
		return common.ErrInternal
	}
	return nil
}

// Find is gorm interface func
func (m *Model) Find(out interface{}, where ...interface{}) error {
	err := m.applyPreloads().db.Find(out, where...).Error
	if err != nil {
		logFields := logrus.Fields{
			"findOut": pretty.Print(out),
			"trace":   common.GetFrames(),
		}
		if len(where) > 0 {
			logFields["findWhere"] = pretty.Print(where)
		}
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logFields).Error("can't find from the database")
		return common.ErrInternal
	}
	return nil
}

// Scan is gorm interface func
func (m *Model) Scan(dest interface{}) error {
	err := m.applyPreloads().db.Scan(dest).Error
	if err != nil {
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logrus.Fields{
			"scanDest": pretty.Print(dest),
			"trace":    common.GetFrames(),
		}).Error("can't scan from the database")
		return common.ErrInternal
	}
	return nil
}

// Create is gorm interface func
func (m *Model) Create(value interface{}) error {
	err := m.applyPreloads().db.Create(value).Error
	if err != nil {
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logrus.Fields{
			"createValue": pretty.Print(value),
			"trace":       common.GetFrames(),
		}).Error("can't create value in database")
		return common.ErrInternal
	}
	return nil
}

// Save is gorm interface func
func (m *Model) Save(value interface{}) error {
	if err := m.applyPreloads().db.Save(value).Error; err != nil {
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logrus.Fields{
			"saveValue": pretty.Print(value),
			"trace":     common.GetFrames(),
		}).Error("can't save object in a database")
		return common.ErrInternal
	}
	return nil
}

// Omit is gorm interface func
func (m *Model) Omit(value ...string) *Model {
	trace := initLogTrace(m.logTrace)
	trace["omit"] = value
	return &Model{db: m.db.Omit(value...), logTrace: trace, preloads: m.preloads}
}

// Updates is gorm interface func
func (m *Model) Updates(attrs interface{}) error {
	if err := m.applyPreloads().db.Updates(attrs).Error; err != nil {
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logrus.Fields{
			"updateAttrs": pretty.Print(attrs),
			"trace":       common.GetFrames(),
		}).Error("can't update object in database")
		return common.ErrInternal
	}
	return nil
}

// Delete is gorm interface func
func (m *Model) Delete(value interface{}, where ...interface{}) error {
	if err := m.applyPreloads().db.Delete(value, where...).Error; err != nil {
		logFields := logrus.Fields{
			"deleteValue": pretty.Print(value),
			"trace":       common.GetFrames(),
		}
		if len(where) > 0 {
			logFields["deleteWhere"] = pretty.Print(where)
		}
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logFields).Error("can't delete object from DB")
		return common.ErrInternal
	}
	return nil
}

// Where is gorm interface func
func (m *Model) Where(query interface{}, args ...interface{}) *Model {
	trace := initLogTrace(m.logTrace)
	var i int
	for {
		if _, ok := trace["whereQuery"+strconv.Itoa(i)]; !ok {
			break
		}
		i++
	}
	trace["whereQuery"+strconv.Itoa(i)] = pretty.Print(query)
	if len(args) > 0 {
		trace["whereArgs"+strconv.Itoa(i)] = pretty.Print(args)
	}
	return &Model{db: m.db.Where(query, args...), logTrace: trace, preloads: m.preloads}
}

// Count is gorm interface func
func (m *Model) Count() (int64, error) {
	var c int64
	if err := m.db.Count(&c).Error; err != nil {
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logrus.Fields{
			"trace": common.GetFrames(),
		}).Error("can't count objects in DB")
		return 0, common.ErrInternal
	}
	return c, nil
}

// Not is gorm interface func
func (m *Model) Not(query interface{}, args ...interface{}) *Model {
	trace := initLogTrace(m.logTrace)
	trace["notQuery"] = pretty.Print(query)
	if len(args) > 0 {
		trace["notArgs"] = args
	}
	return &Model{db: m.db.Not(query, args...), logTrace: trace, preloads: m.preloads}
}

// Group is gorm interface func
func (m *Model) Group(name string) *Model {
	trace := initLogTrace(m.logTrace)
	trace["groupName"] = name
	return &Model{db: m.db.Group(name), logTrace: trace, preloads: m.preloads}
}

// Having is gorm interface func
func (m *Model) Having(query interface{}, args ...interface{}) *Model {
	trace := initLogTrace(m.logTrace)
	trace["havingQuery"] = query
	if len(args) > 0 {
		trace["havingArgs"] = args
	}
	return &Model{db: m.db.Having(query, args...), logTrace: trace, preloads: m.preloads}
}

func (m *Model) exec(sql string, values ...interface{}) error {
	if err := m.applyPreloads().db.Exec(sql, values...).Error; err != nil {
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logrus.Fields{
			"trace":      common.GetFrames(),
			"execSql":    sql,
			"execValues": values,
		}).Error("can't exec sql in DB")
		return common.ErrInternal
	}
	return nil
}

func (m *Model) raw(sql string, values ...interface{}) *Model {
	trace := initLogTrace(m.logTrace)
	trace["rawSql"] = sql
	if len(values) > 0 {
		trace["rawValues"] = values
	}
	return &Model{db: m.db.Raw(sql, values...), logTrace: trace, preloads: m.preloads}
}

// BatchFind is gorm interface func
// FIXME: does not works.
// got error "primary key required" when tried to fetch user followers
// maybe it composite key relates?
func (m *Model) BatchFind(dest interface{}, batchSize int, fc func(tx *Model, batch int) error) error {
	err := m.applyPreloads().db.FindInBatches(dest, batchSize, func(tx *gorm.DB, batch int) error {
		return fc(m, batch)
	}).Error
	if err != nil {
		logFields := logrus.Fields{
			"batchFindDest": pretty.Print(dest),
			"batchSize":     batchSize,
			"trace":         common.GetFrames(),
		}
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logFields).Error("can't find from the database")
		return common.ErrInternal
	}
	return nil
}

// UpdateByFilter is gorm extension. Allow to omit .Model() and .Where() methods
func (m *Model) UpdateByFilter(filter interface{}, values interface{}) error {
	if reflect.DeepEqual(filter, reflect.Zero(reflect.TypeOf(filter)).Interface()) {
		logrus.Error("queryBuilder.UpdateByFilter called for empty filter")
		return common.ErrInternal
	}
	if err := m.applyPreloads().db.Model(filter).Where(filter).Updates(values).Error; err != nil {
		logrus.WithError(err).WithFields(m.logTrace).WithFields(logrus.Fields{
			"UpdateByFilterFilter": pretty.Print(filter),
			"UpdateByFilterValues": pretty.Print(values),
			"trace":                common.GetFrames(),
		}).Error("can't update object in database")
		return common.ErrInternal
	}
	return nil
}
