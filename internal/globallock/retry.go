package globallock

func RunAndRetryRollbacks(fun func() error) error {
	for {
		err := fun()
		if IsRollbackError(err) {
			// Ignore error
			continue
		}
		return err
	}
}
