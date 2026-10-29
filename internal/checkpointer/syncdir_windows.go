//go:build windows

package checkpointer

func syncDir(_ string) error {
	return nil
}
