// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlwrapper

import (
	"context"
	"database/sql"
	"errors"
	"io"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
)

type ConnectionImpl interface {
	driverbase.ConnectionImpl

	// Track a pending operation that blocks other pending operations
	// (generally, a query with a result set, which needs to be cancelled
	// before running other queries on this connection).
	OfferPending(io.Closer) error
	// Cancel any other running queries.
	ClearPending() error
}

// ConnectionImplBase implements the ADBC Connection interface on top of database/sql.
type ConnectionImplBase struct {
	driverbase.ConnectionImplBase
	Derived ConnectionImpl

	// Conn is the dedicated SQL connection for this ADBC session
	Conn *LoggingConn
	// TypeConverter handles SQL-to-Arrow type conversion
	TypeConverter TypeConverter
	// db is the underlying database for metadata operations
	Db *sql.DB

	Pending io.Closer

	// tx is the active transaction when autocommit is disabled, nil otherwise.
	tx *sql.Tx
}

// newConnection creates a new ADBC Connection by acquiring a *sql.Conn from the pool.
func newConnection(ctx context.Context, db *databaseImpl) (adbc.ConnectionWithContext, error) {
	// Acquire a dedicated session
	sqlConn, err := db.db.Conn(ctx)
	if err != nil {
		return nil, db.ErrorHelper.WrapIO(err, "failed to acquire database connection")
	}

	// Set up the driverbase plumbing
	base := driverbase.NewConnectionImplBase(&db.DatabaseImplBase)

	// Create the base sqlwrapper connection first
	sqlwrapperConn := &ConnectionImplBase{
		ConnectionImplBase: base,
		Conn:               &LoggingConn{Conn: sqlConn, Logger: base.Logger, exec: sqlConn},
		TypeConverter:      db.typeConverter,
		Db:                 db.db,
	}

	var impl ConnectionImpl

	// Use custom factory if provided, otherwise use default implementation
	if db.connectionFactory != nil {
		impl, err = db.connectionFactory.CreateConnection(ctx, sqlwrapperConn)
		if err != nil {
			err = errors.Join(err, sqlConn.Close())
			return nil, err
		}
	} else {
		// Default sqlwrapper connection implementation
		impl = sqlwrapperConn
	}
	sqlwrapperConn.Derived = impl

	// Build and return the ADBC Connection wrapper
	builder := driverbase.NewConnectionBuilder(impl)

	// If the implementation supports any of these extras, register them
	if trait, ok := any(impl).(driverbase.AutocommitSetter); ok {
		builder.WithAutocommitSetter(trait)
	}
	if trait, ok := any(impl).(driverbase.CurrentNamespacer); ok {
		builder.WithCurrentNamespacer(trait)
	}
	if trait, ok := any(impl).(driverbase.DbObjectsEnumerator); ok {
		builder.WithDbObjectsEnumerator(trait)
	}
	if trait, ok := any(impl).(driverbase.DbObjectsEnumeratorFactory); ok {
		builder.WithDbObjectsEnumeratorFactory(trait)
	}
	if trait, ok := any(impl).(driverbase.DriverInfoPreparer); ok {
		builder.WithDriverInfoPreparer(trait)
	}
	if trait, ok := any(impl).(driverbase.TableTypeLister); ok {
		builder.WithTableTypeLister(trait)
	}

	return builder.Connection(), nil
}

// NewStatement satisfies adbc.Connection
func (c *ConnectionImplBase) NewStatement(ctx context.Context) (adbc.StatementWithContext, error) {
	return newStatement(c), nil
}

// SetTypeConverter allows higher-level drivers to customize type conversion
func (c *ConnectionImplBase) SetTypeConverter(converter TypeConverter) {
	c.TypeConverter = converter
}

// SetOption sets a string option on this connection
func (c *ConnectionImplBase) SetOption(ctx context.Context, key, value string) error {
	return c.ConnectionImplBase.SetOption(ctx, key, value)
}

func (c *ConnectionImplBase) GetOption(ctx context.Context, key string) (string, error) {
	return c.ConnectionImplBase.GetOption(ctx, key)
}

// SetAutocommit implements driverbase.AutocommitSetter.
func (c *ConnectionImplBase) SetAutocommit(ctx context.Context, enabled bool) error {
	if !enabled {
		if c.tx != nil {
			return nil // already in transaction
		}
		if err := c.ClearPending(); err != nil {
			return err
		}
		return c.beginTx(ctx)
	}
	// Returning to autocommit: commit active transaction
	if c.tx != nil {
		if err := c.ClearPending(); err != nil {
			return err
		}
		err := c.tx.Commit()
		c.tx = nil
		c.Conn.exec = c.Conn.Conn
		if err != nil {
			return c.Base().ErrorHelper.WrapIO(err, "failed to commit on autocommit enable")
		}
	}
	return nil
}

func (c *ConnectionImplBase) beginTx(ctx context.Context) error {
	tx, err := c.Conn.Conn.BeginTx(ctx, nil)
	if err != nil {
		return c.Base().ErrorHelper.WrapIO(err, "failed to begin transaction")
	}
	c.tx = tx
	c.Conn.exec = tx
	return nil
}

// Commit commits the current transaction and begins a new one (ADBC semantics).
func (c *ConnectionImplBase) Commit(ctx context.Context) error {
	if c.tx == nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInvalidState, "no active transaction")
	}
	if err := c.ClearPending(); err != nil {
		return err
	}
	if err := c.tx.Commit(); err != nil {
		c.tx = nil
		c.Conn.exec = c.Conn.Conn
		return c.Base().ErrorHelper.WrapIO(err, "failed to commit transaction")
	}
	c.tx = nil
	c.Conn.exec = c.Conn.Conn
	// ADBC semantics: non-autocommit = always in a transaction
	return c.beginTx(ctx)
}

// Rollback rolls back the current transaction and begins a new one (ADBC semantics).
func (c *ConnectionImplBase) Rollback(ctx context.Context) error {
	if c.tx == nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInvalidState, "no active transaction")
	}
	if err := c.ClearPending(); err != nil {
		return err
	}
	if err := c.tx.Rollback(); err != nil {
		c.tx = nil
		c.Conn.exec = c.Conn.Conn
		return c.Base().ErrorHelper.WrapIO(err, "failed to rollback transaction")
	}
	c.tx = nil
	c.Conn.exec = c.Conn.Conn
	// ADBC semantics: non-autocommit = always in a transaction
	return c.beginTx(ctx)
}

var _ driverbase.AutocommitSetter = (*ConnectionImplBase)(nil)

// Close closes the underlying SQL connection, rolling back any active transaction.
func (c *ConnectionImplBase) Close(ctx context.Context) error {
	if err := c.ClearPending(); err != nil {
		if c.tx != nil {
			_ = c.tx.Rollback()
			c.tx = nil
		}
		return errors.Join(err, c.Conn.Close())
	}
	if c.tx != nil {
		_ = c.tx.Rollback()
		c.tx = nil
	}
	return c.Conn.Close()
}

func (c *ConnectionImplBase) OfferPending(pending io.Closer) error {
	if err := c.ClearPending(); err != nil {
		return err
	}
	c.Pending = pending
	return nil
}

func (c *ConnectionImplBase) ClearPending() error {
	if c.Pending == nil {
		return nil
	}
	defer func() {
		c.Pending = nil
	}()
	if err := c.Pending.Close(); err != nil {
		return c.Base().ErrorHelper.Errorf(adbc.StatusInternal, "failed to clear pending operation: %v", err)
	}
	return nil
}

var _ ConnectionImpl = (*ConnectionImplBase)(nil)
