// Blob is a wrapper around libpurb.Purb
package purbkv

import (
	"go.dedis.ch/kyber/v3/group/curve25519"
	"go.dedis.ch/kyber/v3/util/key"
	"go.dedis.ch/libpurb/libpurb"
	"golang.org/x/xerrors"
	"path/filepath"

	"go.dedis.ch/kyber/v3/util/random"
)

const numberOfRecipients = 1

// NewBlob creates a new blob
func NewBlob(path string) *libpurb.Purb {
	p := libpurb.NewPurb(
		getSuiteInfo(),
		false,
		random.New(),
	)
	p.Recipients = createRecipients(path)

	return p
}

// Encode encodes a slice of bytes into a blob
func Encode(purb *libpurb.Purb, data []byte) ([]byte, error) {
	err := purb.Encode(data)
	blob := purb.ToBytes()

	return blob, err
}

// Decode decodes a blob into a slice of bytes
func Decode(purb *libpurb.Purb, blob []byte) ([]byte, error) {
	success, decrypted, err := purb.Decode(blob)

	if !success {
		err = xerrors.Errorf("Failed to decrypt blob: %v", err)
	}

	return decrypted, err
}

// ---------------------------------------------------------------------------
// helper functions

// see example in libpurb
func getSuiteInfo() libpurb.SuiteInfoMap {
	info := make(libpurb.SuiteInfoMap)
	cornerstoneLength := 32             // defined by Curve 25519
	entryPointLength := 16 + 4 + 4 + 16 // 16-byte symmetric key + 2 * 4-byte offset positions + 16-byte authentication tag
	info[curve25519.NewBlakeSHA256Curve25519(true).String()] = &libpurb.SuiteInfo{
		AllowedPositions: []int{
			12 + 0*cornerstoneLength,
			12 + 1*cornerstoneLength,
			12 + 3*cornerstoneLength,
			12 + 4*cornerstoneLength,
		},
		CornerstoneLength: cornerstoneLength, EntryPointLength: entryPointLength,
	}
	return info
}

// see example in libpurb
func createRecipients(path string) []libpurb.Recipient {
	r := make([]libpurb.Recipient, 0)
	suite := []libpurb.Suite{curve25519.NewBlakeSHA256Curve25519(true)}

	keysPath := filepath.Join(path, "purb.keys")
	loader := NewKeysLoader(keysPath)
	keypair := make([]key.Pair, numberOfRecipients)
	err := loader.Load(&keypair)
	if err != nil {
		// cannot load keys, create new ones
		for i := range keypair {
			keypair[i] = *key.NewKeyPair(suite[0])
		}
		err := loader.Save(&keypair)
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < numberOfRecipients; i++ {
		r = append(r, libpurb.Recipient{
			SuiteName:  suite[0].String(),
			Suite:      suite[0],
			PublicKey:  keypair[i].Public,
			PrivateKey: keypair[i].Private,
		})
	}

	return r
}
