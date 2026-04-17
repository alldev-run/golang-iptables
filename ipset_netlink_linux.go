//go:build linux

package main

import (
	"fmt"
	"net"
	"strings"

	"github.com/vishvananda/netlink"
)

const ipsetBlacklistSetName = "blacklist"

func getIPSetNameForIP(ip string) string {
	parsedIP := net.ParseIP(strings.TrimSpace(ip))
	if parsedIP == nil {
		return "blacklist4"
	}
	if parsedIP.To4() != nil {
		return "blacklist4"
	}
	return "blacklist6"
}

func ipsetAddBlacklistEntry(ip string, banSeconds int) error {
	entry, err := newIPSetEntry(ip)
	if err != nil {
		return err
	}
	entry.Replace = true
	if banSeconds > 0 {
		timeout := uint32(banSeconds)
		entry.Timeout = &timeout
	}
	ipset := getIPSetNameForIP(ip)
	if err := netlink.IpsetAdd(ipset, entry); err != nil {
		if isNetlinkUnsupportedError(err) {
			return errNetlinkUnsupported
		}
		if isIPSetExistError(err) {
			return nil
		}
		return err
	}
	return nil
}

func ipsetDelBlacklistEntry(ip string) error {
	entry, err := newIPSetEntry(ip)
	if err != nil {
		return err
	}
	ipset := getIPSetNameForIP(ip)
	if err := netlink.IpsetDel(ipset, entry); err != nil {
		if isNetlinkUnsupportedError(err) {
			return errNetlinkUnsupported
		}
		if isIPSetExistError(err) {
			return nil
		}
		if isIPSetNoExistError(err) {
			return nil
		}
		return err
	}
	return nil
}

func newIPSetEntry(ip string) (*netlink.IPSetEntry, error) {
	parsedIP := net.ParseIP(strings.TrimSpace(ip))
	if parsedIP == nil {
		return nil, fmt.Errorf("invalid ip: %q", ip)
	}
	if ipv4 := parsedIP.To4(); ipv4 != nil {
		parsedIP = ipv4
	}
	return &netlink.IPSetEntry{IP: parsedIP}, nil
}

func isNetlinkUnsupportedError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "operation not supported") ||
		strings.Contains(msg, "protocol not supported") ||
		strings.Contains(msg, "settype not supported") ||
		strings.Contains(msg, "netlink") && strings.Contains(msg, "not supported")
}

func isIPSetNoExistError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "it's not added") ||
		strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "cannot be deleted") ||
		strings.Contains(msg, "element is missing")
}

func isIPSetExistError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already added") ||
		strings.Contains(msg, "already in set") ||
		msg == "exist" ||
		strings.Contains(msg, "exist")
}
