package kc

type DB string

func (d *DB) Close() {
}

func OpenForWriting(dbfilepath string) (*DB, error) {
	return nil, nil
}
