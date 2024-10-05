package sftpc

import (
	"encoding/base64"
)

type Options func(*SFTPClientParams) error

type SFTPClientParams struct {
	host           string
	port           string
	user           string
	password       string
	privateKeyPath string
	privateKeyB64  []byte
}

func newsSFTPClientParams(opts ...Options) (*SFTPClientParams, error) {
	params := &SFTPClientParams{}
	for _, opt := range opts {
		if err := opt(params); err != nil {
			return nil, err
		}
	}
	return params, nil
}

func WithHost(host string) Options {
	return func(params *SFTPClientParams) error {
		params.host = host
		return nil
	}
}

func WithPort(port string) Options {
	return func(params *SFTPClientParams) error {
		params.port = port
		return nil
	}
}

func WithUser(user string) Options {
	return func(params *SFTPClientParams) error {
		params.user = user
		return nil
	}
}

func WithPassword(password string) Options {
	return func(params *SFTPClientParams) error {
		params.password = password
		return nil
	}
}

func WithPrivateKeyPath(privateKeyPath string) Options {
	return func(params *SFTPClientParams) error {
		params.privateKeyPath = privateKeyPath
		return nil
	}
}

func WithPrivateKeyB64(privateKeyB64 string) Options {
	return func(params *SFTPClientParams) error {
		bytesPrivateKey, err := base64.StdEncoding.DecodeString(privateKeyB64)
		if err != nil {
			return err
		}
		params.privateKeyB64 = bytesPrivateKey
		return nil
	}
}

// getters ----

func (p *SFTPClientParams) Host() string {
	return p.host
}

func (p *SFTPClientParams) Port() string {
	return p.port
}

func (p *SFTPClientParams) User() string {
	return p.user
}

func (p *SFTPClientParams) Password() string {
	return p.password
}

func (p *SFTPClientParams) PrivateKeyPath() string {
	return p.privateKeyPath
}

func (p *SFTPClientParams) PrivateKeyB64() []byte {
	return p.privateKeyB64
}

// setters ----

func (p *SFTPClientParams) SetHost(host string) {
	p.host = host
}

func (p *SFTPClientParams) SetPort(port string) {
	p.port = port
}

func (p *SFTPClientParams) SetUser(user string) {
	p.user = user
}

func (p *SFTPClientParams) SetPassword(password string) {
	p.password = password
}

func (p *SFTPClientParams) SetPrivateKeyPath(privateKeyPath string) {
	p.privateKeyPath = privateKeyPath
}

func (p *SFTPClientParams) SetPrivateKeyB64(privateKeyB64 []byte) {
	p.privateKeyB64 = privateKeyB64
}
