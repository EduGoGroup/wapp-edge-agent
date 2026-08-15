package config

import (
	"net"
	"net/url"
)

// PlatformAPIBaseURLInsecure reporta si baseURL mandaría una contraseña en CLARO por la red al llamar al
// signup público de la plataforma (Trabajo 1, code review 056 · T11): true cuando el esquema es "http" Y
// el host NO es loopback (localhost, 127.0.0.1, ::1). "https://" siempre se permite (va cifrado
// independientemente del host) y loopback siempre se permite (el tráfico nunca sale de esta máquina,
// aunque el esquema sea "http"). Una URL que no parsea, o sin esquema/host, se trata como INSEGURA
// (fail-closed): mejor rechazar un valor ilegible que dejarlo pasar sin comprobar.
func PlatformAPIBaseURLInsecure(baseURL string) bool {
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme == "" || u.Hostname() == "" {
		return true
	}
	if u.Scheme != "http" {
		return false
	}
	return !isLoopbackHost(u.Hostname())
}

// isLoopbackHost reporta si host es "localhost" o una IP de loopback (127.0.0.0/8, ::1).
func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
