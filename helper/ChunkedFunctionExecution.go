package projection

// ExecuteFunctionChunkWise execute a given a function as long as the function returns true.
func ExecuteFunctionChunkWise(execute func() (bool, error)) (err error) {
	var reRun bool
	for ok := true; ok; ok = reRun {
		reRun, err = execute()
		if err != nil {
			return err
		}
	}
	return nil
}
