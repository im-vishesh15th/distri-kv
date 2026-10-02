package gateway

import (
	"context"
	"errors"
	"time"
)

// CreateAdminUser creates an operator account (no tenant) that can use the
// /api/v1/admin/* routes. It is used by `gatewayctl create-admin`; there is
// deliberately no HTTP route that can create an operator.
func CreateAdminUser(ctx context.Context, s AccountStore, email, password string, iter int, now time.Time) (User, error) {
	e, ok := normalizeEmail(email)
	if !ok {
		return User{}, errors.New("gateway: invalid email address")
	}
	if err := ValidatePassword(password); err != nil {
		return User{}, err
	}
	hash, err := HashPassword(password, iter)
	if err != nil {
		return User{}, err
	}
	id, err := randHex(16)
	if err != nil {
		return User{}, err
	}
	u := User{ID: id, Email: e, PasswordHash: hash, IsAdmin: true, CreatedAt: now}
	if err := s.CreateAccount(ctx, u, nil); err != nil {
		return User{}, err
	}
	return u, nil
}

// ResetUserPassword sets a new password for the account with this email and
// signs the user out everywhere. Used by `gatewayctl reset-password`.
func ResetUserPassword(ctx context.Context, s AccountStore, email, password string, iter int) error {
	e, ok := normalizeEmail(email)
	if !ok {
		return errors.New("gateway: invalid email address")
	}
	if err := ValidatePassword(password); err != nil {
		return err
	}
	u, err := s.GetUserByEmail(ctx, e)
	if err != nil {
		return err
	}
	hash, err := HashPassword(password, iter)
	if err != nil {
		return err
	}
	if err := s.SetPassword(ctx, u.ID, hash); err != nil {
		return err
	}
	return s.DeleteUserSessions(ctx, u.ID, "")
}
