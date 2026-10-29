//go:build windows

package journal

const openNoFollow = 0

func syncDir(_ string) error {
	return nil
}
