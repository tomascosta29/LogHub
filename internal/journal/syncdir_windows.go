//go:build windows

package journal

func syncDir(_ string) error {
	return nil
}
