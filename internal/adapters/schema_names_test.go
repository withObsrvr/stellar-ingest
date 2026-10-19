package adapters

// The store schema and the view schema cannot be the same.
//
// A table and a view cannot share a name within one schema, and every one of
// the 21 views is named after the table it reads. Configured the same way,
// the run creates 21 tables and then fails on the first view -- late, and
// with a message about a view rather than about the configuration.

import "testing"

func TestSchemaNamesMustDiffer(t *testing.T) {
	for _, tc := range []struct {
		name        string
		store, view string
		wantErr     bool
	}{
		{"the defaults", "bronze_store", "bronze", false},
		{"isolated validation names", "val_store", "val_views", false},
		{"same schema", "bronze", "bronze", true},
		{"same non-default schema", "lake", "lake", true},
		{"empty store", "", "bronze", true},
		{"empty view", "bronze_store", "", true},
	} {
		err := validateSchemaNames(tc.store, tc.view)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: validateSchemaNames(%q, %q) error = %v, wantErr %v",
				tc.name, tc.store, tc.view, err, tc.wantErr)
		}
	}
}

// The pusher applies it, so a bad pair cannot reach table creation.
func TestPusherRejectsMatchingSchemas(t *testing.T) {
	if _, err := NewDuckLakePusher(DuckLakeConfig{
		SchemaName: "bronze", ViewSchemaName: "bronze",
	}); err == nil {
		t.Error("the pusher accepted identical store and view schemas")
	}
}

// And the defaults are a valid pair, which is what every run without flags
// gets.
func TestDefaultSchemaNamesAreValid(t *testing.T) {
	if err := validateSchemaNames("bronze_store", "bronze"); err != nil {
		t.Errorf("the defaults are rejected: %v", err)
	}
}
