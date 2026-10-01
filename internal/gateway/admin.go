package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Key format: "dkv_live_" + 64 hex chars (256 bits of randomness).
// The first 8 hex chars after the prefix are the displayable key ID.
const (
	KeyPrefix     = "dkv_live_"
	keyRandHex    = 64
	keyLen        = len(KeyPrefix) + keyRandHex
	displayPrefix = len(KeyPrefix) + 8
)

func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// GenerateKey returns a new random plaintext API key.
func GenerateKey() (string, error) {
	raw := make([]byte, keyRandHex/2)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("gateway: generate key: %w", err)
	}
	return KeyPrefix + hex.EncodeToString(raw), nil
}

// CreateTenant validates the ID and stores a new tenant.
func CreateTenant(ctx context.Context, s Store, id, name string, now time.Time) (Tenant, error) {
	if err := ValidateTenantID(id); err != nil {
		return Tenant{}, err
	}
	if strings.TrimSpace(name) == "" {
		name = id
	}
	t := Tenant{ID: id, Name: name, CreatedAt: now}
	if err := s.CreateTenant(ctx, t); err != nil {
		return Tenant{}, err
	}
	return t, nil
}

// CreateAPIKey stores a new key for tenantID and returns the plaintext key.
// The plaintext is returned exactly once; only its hash is stored.
// ttl == 0 means the key never expires; negative ttl is rejected.
func CreateAPIKey(ctx context.Context, s Store, tenantID, name string, ttl time.Duration, now time.Time) (string, APIKey, error) {
	if ttl < 0 {
		return "", APIKey{}, errors.New("gateway: negative ttl")
	}
	if _, err := s.GetTenant(ctx, tenantID); err != nil {
		return "", APIKey{}, err
	}
	for attempt := 0; attempt < 5; attempt++ {
		plain, err := GenerateKey()
		if err != nil {
			return "", APIKey{}, err
		}
		ak := APIKey{
			Hash:      hashKey(plain),
			Prefix:    plain[:displayPrefix],
			TenantID:  tenantID,
			Name:      name,
			CreatedAt: now,
		}
		if ttl > 0 {
			exp := now.Add(ttl)
			ak.ExpiresAt = &exp
		}
		err = s.CreateKey(ctx, ak)
		if errors.Is(err, ErrAPIKeyExists) {
			continue // display-prefix collision: draw again
		}
		if err != nil {
			return "", APIKey{}, err
		}
		return plain, ak, nil
	}
	return "", APIKey{}, errors.New("gateway: could not generate a unique key")
}

// RotateAPIKey creates a replacement key (same name and tenant) and revokes
// the old one. If creation fails the old key stays active.
func RotateAPIKey(ctx context.Context, s Store, tenantID, oldPrefix string, ttl time.Duration, now time.Time) (string, APIKey, error) {
	keys, err := s.ListKeys(ctx, tenantID)
	if err != nil {
		return "", APIKey{}, err
	}
	var name string
	found := false
	for _, k := range keys {
		if k.Prefix == oldPrefix {
			name, found = k.Name, true
			break
		}
	}
	if !found {
		return "", APIKey{}, ErrAPIKeyNotFound
	}
	plain, ak, err := CreateAPIKey(ctx, s, tenantID, name, ttl, now)
	if err != nil {
		return "", APIKey{}, err
	}
	if err := s.RevokeKey(ctx, tenantID, oldPrefix, now); err != nil {
		return "", APIKey{}, err
	}
	return plain, ak, nil
}
