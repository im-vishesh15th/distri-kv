package gateway

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Password hashing: PBKDF2-HMAC-SHA256, implemented here so the project needs
// no new module (x/crypto is not in go.sum). Stored form:
//
//	pbkdf2-sha256$<iterations>$<salt b64>$<hash b64>
//
// The iteration count is stored with the hash, so it can be raised later and
// old hashes still verify.

const (
	defaultPBKDF2Iter = 600_000 // OWASP guidance for PBKDF2-HMAC-SHA256
	maxPBKDF2Iter     = 5_000_000
	pbkdf2SaltLen     = 16
	pbkdf2KeyLen      = 32

	minPasswordLen = 10
	maxPasswordLen = 128
)

var ErrBadPasswordHash = errors.New("gateway: malformed password hash")

// pbkdf2SHA256 is RFC 8018 PBKDF2 with HMAC-SHA256 as the PRF.
func pbkdf2SHA256(password, salt []byte, iter, keyLen int) []byte {
	prf := hmac.New(sha256.New, password)
	hLen := prf.Size()
	blocks := (keyLen + hLen - 1) / hLen
	out := make([]byte, 0, blocks*hLen)
	var ctr [4]byte
	u := make([]byte, hLen)
	for b := 1; b <= blocks; b++ {
		prf.Reset()
		prf.Write(salt)
		binary.BigEndian.PutUint32(ctr[:], uint32(b))
		prf.Write(ctr[:])
		u = prf.Sum(u[:0])
		t := append([]byte(nil), u...)
		for i := 1; i < iter; i++ {
			prf.Reset()
			prf.Write(u)
			u = prf.Sum(u[:0])
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

// HashPassword returns a salted PBKDF2 hash string for password.
func HashPassword(password string, iter int) (string, error) {
	if iter <= 0 {
		iter = defaultPBKDF2Iter
	}
	salt := make([]byte, pbkdf2SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("gateway: salt: %w", err)
	}
	dk := pbkdf2SHA256([]byte(password), salt, iter, pbkdf2KeyLen)
	enc := base64.RawStdEncoding
	return "pbkdf2-sha256$" + strconv.Itoa(iter) + "$" + enc.EncodeToString(salt) + "$" + enc.EncodeToString(dk), nil
}

// VerifyPassword reports whether password matches the stored hash, in
// constant time with respect to the hash value.
func VerifyPassword(password, stored string) (bool, error) {
	parts := strings.Split(stored, "$")
	if len(parts) != 4 || parts[0] != "pbkdf2-sha256" {
		return false, ErrBadPasswordHash
	}
	iter, err := strconv.Atoi(parts[1])
	if err != nil || iter < 1 || iter > maxPBKDF2Iter {
		return false, ErrBadPasswordHash
	}
	enc := base64.RawStdEncoding
	salt, err1 := enc.DecodeString(parts[2])
	want, err2 := enc.DecodeString(parts[3])
	if err1 != nil || err2 != nil || len(want) == 0 {
		return false, ErrBadPasswordHash
	}
	got := pbkdf2SHA256([]byte(password), salt, iter, len(want))
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

// ValidatePassword enforces the signup/change policy: length only (NIST-style;
// no composition rules).
func ValidatePassword(p string) error {
	switch {
	case len(p) < minPasswordLen:
		return fmt.Errorf("password must be at least %d characters", minPasswordLen)
	case len(p) > maxPasswordLen:
		return fmt.Errorf("password must be at most %d characters", maxPasswordLen)
	}
	return nil
}
