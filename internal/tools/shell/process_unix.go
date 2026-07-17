//go:build darwin || linux

package shell

import (
	"errors"
	"os/exec"
	"syscall"
	"time"
)

func configureProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminateProcessGroup(command *exec.Cmd) {
	if command.Process == nil {
		return
	}
	processGroup := -command.Process.Pid
	_ = syscall.Kill(processGroup, syscall.SIGTERM)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(processGroup, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = syscall.Kill(processGroup, syscall.SIGKILL)
}
