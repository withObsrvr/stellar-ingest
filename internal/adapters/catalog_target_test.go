package adapters

// Which catalog DuckLake attaches is decided by a prefix, and getting it
// wrong is not a subtle failure -- a path sent as `postgres:/tmp/x.ducklake`
// is a connection attempt, and a DSN sent bare is a filename. The decision is
// here rather than inline in Setup so it can be read without a database.

import "testing"

func TestDuckLakeCatalogTarget(t *testing.T) {
	for _, tc := range []struct {
		name string
		dsn  string
		want string
	}{
		{
			"postgres URL, which is what production uses",
			"postgresql://obsrvr:pw@127.0.0.1:5432/obsrvr_lake_catalog_prod?sslmode=disable",
			"ducklake:postgres:postgresql://obsrvr:pw@127.0.0.1:5432/obsrvr_lake_catalog_prod?sslmode=disable",
		},
		{
			"postgres:// short scheme",
			"postgres://user@host/db",
			"ducklake:postgres:postgres://user@host/db",
		},
		{
			"key=value postgres DSN",
			"host=127.0.0.1 user=obsrvr dbname=catalog",
			"ducklake:postgres:host=127.0.0.1 user=obsrvr dbname=catalog",
		},
		{
			"a DuckDB file, which is the point of this function",
			"/data/catalog.ducklake",
			"ducklake:/data/catalog.ducklake",
		},
		{
			"a relative DuckDB file",
			"catalog.ducklake",
			"ducklake:catalog.ducklake",
		},
		{
			"sqlite keeps its own prefix",
			"sqlite:/data/catalog.sqlite",
			"ducklake:sqlite:/data/catalog.sqlite",
		},
		{
			"an explicit target passes through untouched",
			"ducklake:postgres:host=x",
			"ducklake:postgres:host=x",
		},
	} {
		if got := duckLakeCatalogTarget(tc.dsn); got != tc.want {
			t.Errorf("%s:\n  dsn  = %q\n  got  = %q\n  want = %q", tc.name, tc.dsn, got, tc.want)
		}
	}
}

// The production DSN must still resolve to exactly what it resolved to before
// this function existed. Everything in cold was written through that string.
func TestProductionDSNIsUnchanged(t *testing.T) {
	const dsn = "postgresql://obsrvr:pw@127.0.0.1:5432/obsrvr_lake_catalog_prod?sslmode=disable"
	if got, want := duckLakeCatalogTarget(dsn), "ducklake:postgres:"+dsn; got != want {
		t.Errorf("the production catalog target changed:\n  got  %q\n  want %q", got, want)
	}
}

// A path must never be turned into a connection string. That mistake reads as
// a database that will not connect rather than as a misrouted filename.
func TestPathsAreNeverSentToPostgres(t *testing.T) {
	for _, path := range []string{
		"/data/catalog.ducklake",
		"./catalog.db",
		"/var/lib/obsrvr/lake.duckdb",
	} {
		if got := duckLakeCatalogTarget(path); got != "ducklake:"+path {
			t.Errorf("path %q became %q", path, got)
		}
	}
}
