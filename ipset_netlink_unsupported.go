//go:build !linux

package main

func ipsetAddBlacklistEntry(ip string, banSeconds int) error {
	return errNetlinkUnsupported
}

func ipsetDelBlacklistEntry(ip string) error {
	return errNetlinkUnsupported
}
