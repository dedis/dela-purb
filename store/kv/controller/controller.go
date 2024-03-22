// Package controller implements a CLI controller for the key/value database.
//
// Documentation Last Review: 08.10.2020
package controller

import (
	"go.dedis.ch/dela/cli"
	"go.dedis.ch/dela/cli/node"
	"go.dedis.ch/purb-db/store/kv"
	"golang.org/x/xerrors"
)

// MinimalController is a CLI controller to inject a key/value database.
//
// - implements node.Initializer
type minimalController struct {
	purbIsOn bool
}

// NewController returns a minimal controller
// that will inject a key/value database.
func NewController() node.Initializer {
	return minimalController{
		purbIsOn: true,
	}
}

// NewControllerWithoutPurb returns a minimal controller without PURB
// that will inject a key/value database.
func NewControllerWithoutPurb() node.Initializer {
	return minimalController{
		purbIsOn: false,
	}
}

// SetCommands implements node.Initializer. It does not register any command.
func (m minimalController) SetCommands(builder node.Builder) {}

// OnStart implements node.Initializer. It opens the database in a file using
// the config path as the base.
func (m minimalController) OnStart(flags cli.Flags, inj node.Injector) error {
	db, err := purbkv.NewDB(flags.String("config"), m.purbIsOn)
	if err != nil {
		return xerrors.Errorf("db: %v", err)
	}

	inj.Inject(db)

	return nil
}

// OnStop implements node.Initializer. It closes the database.
func (m minimalController) OnStop(inj node.Injector) error {
	var db purbkv.DB
	err := inj.Resolve(&db)
	if err != nil {
		return xerrors.Errorf("injector: %v", err)
	}

	err = db.Close()
	if err != nil {
		return xerrors.Errorf("while closing db: %v", err)
	}

	return nil
}
