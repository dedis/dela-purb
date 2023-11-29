package controller

import (
	"github.com/stretchr/testify/require"
	"go.dedis.ch/dela/cli/node"
	"testing"
)

func TestNewController(t *testing.T) {
	c := NewController()
	require.NotNil(t, c)

	require.Equal(t, minimalController{true}, c)
}

func TestNewControllerWithoutPurb(t *testing.T) {
	c := NewControllerWithoutPurb()
	require.NotNil(t, c)

	require.Equal(t, minimalController{false}, c)
}

func TestOnStart(t *testing.T) {
	c := NewController()

	err := c.OnStart(node.FlagSet{}, node.NewInjector())
	require.NoError(t, err)
}

func TestOnStop(t *testing.T) {
	c := NewController()

	inj := node.NewInjector()

	err := c.OnStart(node.FlagSet{}, inj)
	require.NoError(t, err)

	err = c.OnStop(inj)
	require.NoError(t, err)
}

func TestSetCommands(t *testing.T) {
	c := NewController()

	c.SetCommands(nil)
}
