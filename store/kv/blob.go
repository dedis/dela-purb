package kv

import (
	"go.dedis.ch/kyber/v3/group/curve25519"
	"go.dedis.ch/kyber/v3/util/key"
	"go.dedis.ch/libpurb/libpurb"
	"golang.org/x/xerrors"

	"go.dedis.ch/kyber/v3/util/random"
)

type Blob struct {
	purb *libpurb.Purb
}

// NewBlob creates a new blob
func NewBlob() *Blob {
	suitesInfo := getSuiteInfo()
	simplified := true

	p := libpurb.NewPurb(
		suitesInfo,
		simplified,
		random.New(),
	)

	p.Recipients = createRecipients(1)

	return &Blob{
		purb: p,
	}
}

// Encode encodes a slice of bytes into a blob
func (b *Blob) Encode(data []byte) ([]byte, error) {
	err := b.purb.Encode(data)
	blob := b.purb.ToBytes()

	return blob, err
}

// Decode decodes a blob into a slice of bytes
func (b *Blob) Decode(blob []byte) ([]byte, error) {
	success, decrypted, err := b.purb.Decode(blob)

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
func createRecipients(n int) []libpurb.Recipient {
	decs := make([]libpurb.Recipient, 0)
	suites := []libpurb.Suite{curve25519.NewBlakeSHA256Curve25519(true)}
	for _, suite := range suites {
		for i := 0; i < n; i++ {
			pair := key.NewKeyPair(suite)
			decs = append(decs, libpurb.Recipient{
				SuiteName:  suite.String(),
				Suite:      suite,
				PublicKey:  pair.Public,
				PrivateKey: pair.Private,
			})
		}
	}
	return decs
}
