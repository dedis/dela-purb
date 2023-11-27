package kv

import (
	"github.com/stretchr/testify/require"
	"go.dedis.ch/kyber/v3/group/curve25519"
	"go.dedis.ch/kyber/v3/util/key"
	"os"
	"path/filepath"
	"testing"
)

const keyTestDir = "key-loader"
const keyTestFile = "test.keys"

func TestNewKeysLoader(t *testing.T) {
	keyDir, err := os.MkdirTemp(os.TempDir(), keyTestDir)
	require.NoError(t, err)

	keyPath := filepath.Join(keyDir, keyTestFile)
	defer os.RemoveAll(keyPath)

	loader := NewKeysLoader(keyPath)
	require.Equal(t, loader.path, keyPath)
}

func TestKeysloaderSave(t *testing.T) {
	keyDir, err := os.MkdirTemp(os.TempDir(), keyTestDir)
	require.NoError(t, err)

	keyPath := filepath.Join(keyDir, keyTestFile)
	defer os.RemoveAll(keyPath)

	loader := NewKeysLoader(keyPath)

	err = loader.Save(nil)
	require.Error(t, err)

	err = loader.Save(&[]key.Pair{*key.NewKeyPair(curve25519.NewBlakeSHA256Curve25519(true))})
	require.NoError(t, err)

	err = loader.Save(&[]key.Pair{
		*key.NewKeyPair(curve25519.NewBlakeSHA256Curve25519(true)),
		*key.NewKeyPair(curve25519.NewBlakeSHA256Curve25519(true)),
	})
	require.NoError(t, err)
}

func TestKeysloaderLoad(t *testing.T) {
	keyDir, err := os.MkdirTemp(os.TempDir(), keyTestDir)
	require.NoError(t, err)

	keyPath := filepath.Join(keyDir, keyTestFile)
	defer os.RemoveAll(keyPath)

	loader := NewKeysLoader(keyPath)

	keypair := []key.Pair{
		*key.NewKeyPair(curve25519.NewBlakeSHA256Curve25519(true)),
		*key.NewKeyPair(curve25519.NewBlakeSHA256Curve25519(true)),
	}

	err = loader.Save(&keypair)
	require.NoError(t, err)

	loadedKeypair := make([]key.Pair, 2)
	err = loader.Load(&loadedKeypair)
	require.NoError(t, err)

	for i, kp := range loadedKeypair {
		require.Equal(t, keypair[i].Public, kp.Public)
	}
}
