package migrations

import "embed"

//go:embed storage/*.sql
var StorageFS embed.FS

//go:embed bucket/*.sql
var BucketFS embed.FS
