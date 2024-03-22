package purbkv

import (
	"bufio"
	"encoding/base64"
	"go.dedis.ch/kyber/v3/group/curve25519"
	"go.dedis.ch/kyber/v3/util/key"
	"golang.org/x/xerrors"
	"os"
	"strings"
)

// FileLoader is loader that is storing the new keys to a file.
//
// - implements loader.Loader
type fileLoader struct {
	path string

	openFileFn func(path string, flags int, perms os.FileMode) (*os.File, error)
	statFn     func(path string) (os.FileInfo, error)
}

// NewKeyLoader creates a new key file loader using the given file path.
func NewKeysLoader(path string) fileLoader {
	return fileLoader{
		path:       path,
		openFileFn: os.OpenFile,
		statFn:     os.Stat,
	}
}

// Load loads the keys from the file if it exists,
// otherwise it returns an error.
func (l fileLoader) Load(keypair *[]key.Pair) error {
	file, err := l.openFileFn(l.path, os.O_RDONLY, 0400)
	if err != nil {
		return xerrors.Errorf("while opening file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	i := 0
	for scanner.Scan() {
		line := scanner.Text()
		keys := strings.Split(line, ":")
		if len(keys) != 2 {
			return xerrors.Errorf("invalid key format")
		}

		pubk, err := base64.URLEncoding.DecodeString(keys[0])
		if err != nil {
			return xerrors.Errorf("while decoding pubk in %v: %v", line, err)
		}

		privk, err := base64.URLEncoding.DecodeString(keys[1])
		if err != nil {
			return xerrors.Errorf("while decoding privk in %v: %v", line, err)
		}

		kp := *key.NewKeyPair(curve25519.NewBlakeSHA256Curve25519(true))

		err = kp.Public.UnmarshalBinary(pubk)
		if err != nil {
			return xerrors.Errorf("while unmarshaling pubk in %v: %v", line, err)
		}

		err = kp.Private.UnmarshalBinary(privk)
		if err != nil {
			return xerrors.Errorf("while unmarshaling privk in %v: %v", line, err)
		}

		(*keypair)[i] = kp
		i++
		if i == len(*keypair) {
			break
		}
	}

	if i != len(*keypair) {
		return xerrors.Errorf("number of keys does not match")
	}

	return nil
}

// Save the keys to the file in path,
// otherwise it returns an error
func (l fileLoader) Save(keypair *[]key.Pair) error {
	if keypair == nil {
		return xerrors.Errorf("keypair is nil")
	}

	if len(*keypair) == 0 {
		return xerrors.Errorf("number of keys is 0")
	}

	file, err := l.openFileFn(l.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return xerrors.Errorf("while creating file: %v", err)
	}
	defer file.Close()

	for _, k := range *keypair {
		pubk, err := k.Public.MarshalBinary()
		if err != nil {
			return xerrors.Errorf("while marshaling pubk: %v", err)
		}
		pubkString := base64.URLEncoding.EncodeToString(pubk)

		privk, err := k.Private.MarshalBinary()
		if err != nil {
			return xerrors.Errorf("while marshaling privk: %v", err)
		}
		privkString := base64.URLEncoding.EncodeToString(privk)

		_, err = file.WriteString(pubkString + ":" + privkString + "\n")
		if err != nil {
			return xerrors.Errorf("while writing pubk:privk {%v:%v} to disk: %v", pubkString, privkString, err)
		}
	}

	if err != nil {
		return xerrors.Errorf("while encoding: %v", err)
	}

	return nil
}
