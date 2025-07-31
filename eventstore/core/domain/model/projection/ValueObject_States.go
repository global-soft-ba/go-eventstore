package projection

import (
	"fmt"
)

type State string

const (
	Running    State = "Running"
	Stopped    State = "Stopped"
	Rebuilding State = "Rebuilding"
	Erroneous  State = "Erroneous"
)

func (s State) isValidProjectionStateChange(target State) error {
	switch s {
	case Running:
		if target == Running || target == Stopped || target == Rebuilding || target == Erroneous {
			return nil
		}
	case Stopped:
		if target == Stopped || target == Running || target == Rebuilding {
			return nil
		}
	case Rebuilding:
		if target == Stopped || target == Erroneous {
			return nil
		}
	case Erroneous:
		if target == Stopped {
			return nil
		}
	}

	return fmt.Errorf("change of state %q to state %q is not allowed", s, target)
}
