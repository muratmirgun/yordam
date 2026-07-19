//go:build !darwin && !linux

package recovery

import (
	"context"
	"fmt"
	"os"
)

type recoveryCoordination struct{}

func acquireRecoveryCoordination(context.Context, *os.Root) (*recoveryCoordination, error) {
	return nil, fmt.Errorf("%w: cross-process coordination is unsupported on this platform", ErrRecoveryBusy)
}

func (*recoveryCoordination) release() error { return nil }
