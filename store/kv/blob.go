// Blob is a wrapper around libpurb.Purb
package kv

import (
	"go.dedis.ch/kyber/v3/group/curve25519"
	"go.dedis.ch/kyber/v3/util/key"
	"go.dedis.ch/libpurb/libpurb"
	"golang.org/x/xerrors"

	"go.dedis.ch/kyber/v3/util/random"
)

const numberOfRecipients = 1

// NewBlob creates a new blob
func NewBlob(keypair []key.Pair) *libpurb.Purb {
	p := libpurb.NewPurb(
		getSuiteInfo(),
		false,
		random.New(),
	)
	p.Recipients = createRecipients(keypair)

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
func createRecipients(keypair []key.Pair) []libpurb.Recipient {
	r := make([]libpurb.Recipient, 0)
	suites := []libpurb.Suite{curve25519.NewBlakeSHA256Curve25519(true)}

	if len(keypair) < numberOfRecipients {
		keypair = make([]key.Pair, 0)
	}

	for _, suite := range suites {
		for i := 0; i < numberOfRecipients; i++ {
			if len(keypair) < numberOfRecipients {
				keypair = append(keypair, *key.NewKeyPair(suite))
			}

			r = append(r, libpurb.Recipient{
				SuiteName:  suite.String(),
				Suite:      suite,
				PublicKey:  keypair[i].Public,
				PrivateKey: keypair[i].Private,
			})
		}
	}
	return r
}
