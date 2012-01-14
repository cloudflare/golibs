package kc

type DB string

func (d *DB) Close() error {
	return nil
}

func OpenForWriting(dbfilepath string) (*DB, error) {
	return nil, nil
}
