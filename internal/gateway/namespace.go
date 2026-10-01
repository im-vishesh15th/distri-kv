package gateway

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Tenant isolation lives here, in one place.
//
// Every key a customer sends is rewritten to
//
//	"t:" + tenantID + ":" + key
//
// The encoding is injective (two different (tenantID, key) pairs can never
// produce the same string) because a tenant ID may NOT contain ':'. The first
// ':' after the "t:" prefix therefore always ends the tenant ID, and
// everything after it is the customer's key, whatever bytes it contains.
//
// The old encoding "tenant:" + id + ":" + key was only injective if tenant IDs
// were validated, and they were not: tenant "a" + key "b:c" and tenant "a:b" +
// key "c" both became "tenant:a:b:c".

const (
	maxTenantIDLen = 63
	maxKeyLen      = 512
	nsPrefix       = "t:"
)

var (
	ErrInvalidTenantID = errors.New("invalid tenant id: must match ^[a-z0-9][a-z0-9_-]{0,62}$")
	ErrInvalidKey      = errors.New("invalid key: must be 1-512 bytes of valid UTF-8")

	tenantIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
)

// ValidateTenantID reports whether id is a legal tenant identifier.
func ValidateTenantID(id string) error {
	if len(id) == 0 || len(id) > maxTenantIDLen || !tenantIDRe.MatchString(id) {
		return ErrInvalidTenantID
	}
	return nil
}

// ValidateKey reports whether key is a legal customer-supplied key.
func ValidateKey(key string) error {
	if len(key) == 0 || len(key) > maxKeyLen || !utf8.ValidString(key) {
		return ErrInvalidKey
	}
	return nil
}

// NamespaceKey validates both parts and returns the storage key. This is the
// only function that should build keys for the data plane.
func NamespaceKey(tenantID, key string) (string, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return "", err
	}
	if err := ValidateKey(key); err != nil {
		return "", err
	}
	return nsPrefix + tenantID + ":" + key, nil
}

// splitNamespaced is the inverse of NamespaceKey. It exists for tests and
// debugging tools; the data path never needs it.
func splitNamespaced(ns string) (tenantID, key string, err error) {
	if !strings.HasPrefix(ns, nsPrefix) {
		return "", "", fmt.Errorf("gateway: %q has no namespace prefix", ns)
	}
	rest := ns[len(nsPrefix):]
	i := strings.IndexByte(rest, ':')
	if i < 0 {
		return "", "", fmt.Errorf("gateway: %q has no tenant delimiter", ns)
	}
	return rest[:i], rest[i+1:], nil
}
