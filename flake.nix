{
  description = "stellar-ingest — transform historical Stellar ledger data into analytical datasets";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Read from the Go source rather than restated here. The two had
        # already drifted once -- the flake still said 0.1.0 while
        # internal/domain.SemVer had moved on -- and a nix build quietly
        # naming the wrong release is exactly the kind of mismatch
        # release-check exists to catch everywhere else. A regex that stops
        # matching fails the evaluation loudly, which is the right failure.
        version = builtins.head (builtins.match
          ".*\n*var SemVer = \"([^\"]+)\".*"
          (builtins.readFile ./internal/domain/version.go));
        revision = self.shortRev or self.dirtyShortRev or "dev";

        stellar-ingest = pkgs.buildGoModule {
          pname = "stellar-ingest";
          inherit version;

          src = self;

          # Content hash of the fetched Go module set. It is derived from
          # go.mod and go.sum, so ANY dependency change invalidates it and
          # the nix build fails with a hash mismatch — `make test` will not
          # notice, because it never runs nix.
          #
          # Regenerate with `make nix-vendor-hash`.
          vendorHash = "sha256-5pWW1Wgz3KtQmIlXKedi3ogDHINXtkXXninwGqlaGQo=";

          # duckdb-go/v2 is cgo and bundles its own DuckDB, so unlike the other
          # Obsrvr services this cannot be built with CGO_ENABLED=0.
          env.CGO_ENABLED = "1";

          nativeBuildInputs = [ pkgs.pkg-config ];

          # BuildDate is deliberately left unset here. A flake build has no
          # commit date available (the source is a store path, not a git
          # tree), and inventing one would make `version` claim a precision
          # the build does not have. Empty omits the field.
          ldflags = [ "-X" "github.com/withObsrvr/stellar-ingest/internal/domain.Version=${revision}" ];

          # The module is a single flat package main at the repository root.
          subPackages = [ "cmd/stellar-ingest" ];

          # The golden Bronze suite is hermetic — fixtures are committed and it
          # needs no network or credentials — so it runs as part of the build.
          # Only the DuckLake integration tests need outside resources, and they
          # skip themselves when their environment variables are unset.
          doCheck = true;

          # Linking DuckDB is slow; a short default timeout would flake.
          checkFlags = [ "-timeout=20m" ];

          preCheck = ''
            # ENABLE_FLOWCTL leaking in would make standalone runs fail closed.
            unset ENABLE_FLOWCTL FLOWCTL_ENDPOINT
            unset STORAGE_TYPE BUCKET_NAME BUCKET_PATH RPC_ENDPOINT
          '';

          # stellar-history-loader is the transitional alias for the same
          # program, kept so existing operational scripts keep working.
          postInstall = ''
            ln -s stellar-ingest $out/bin/stellar-history-loader
            install -Dm444 internal/adapters/v3_bronze_schema.sql $out/share/stellar-ingest/v3_bronze_schema.sql
            install -Dm444 internal/adapters/v3_bronze_views.sql $out/share/stellar-ingest/v3_bronze_views.sql
          '';

          meta = with pkgs.lib; {
            description = "Transform historical Stellar ledger data into analytical datasets";
            homepage = "https://github.com/withObsrvr/stellar-ingest";
            license = licenses.asl20;
            mainProgram = "stellar-ingest";
            platforms = platforms.unix;
          };
        };
      in
      {
        packages = {
          default = stellar-ingest;
          inherit stellar-ingest;

          docker = pkgs.dockerTools.buildLayeredImage {
            name = "withobsrvr/stellar-ingest";
            tag = "latest";

            contents = [
              stellar-ingest
              pkgs.cacert
              pkgs.tzdata
            ];

            config = {
              Entrypoint = [ "${stellar-ingest}/bin/stellar-ingest" ];
              Env = [
                "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
                "TZDIR=${pkgs.tzdata}/share/zoneinfo"
              ];
              WorkingDir = "/data";
              User = "1000:1000";
            };

            extraCommands = ''
              mkdir -p data tmp
            '';
          };
        };

        apps = {
          default = flake-utils.lib.mkApp {
            drv = stellar-ingest;
            name = "stellar-ingest";
          };
          stellar-history-loader = flake-utils.lib.mkApp {
            drv = stellar-ingest;
            name = "stellar-history-loader";
          };
        };

        # `nix flake check` builds the package, which runs the hermetic test
        # suite including the golden Bronze snapshots.
        checks.stellar-ingest = stellar-ingest;

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            go
            gopls
            gotools
            delve
            golangci-lint

            # CGO toolchain for duckdb-go/v2.
            gcc
            pkg-config

            # duckdb CLI: inspects Parquet output and backs the golden harness
            # workflow. zstd: compresses and decompresses ledger fixtures.
            duckdb
            zstd

            # Fixtures are fetched from the pubnet GCS archive.
            google-cloud-sdk

            git
            gnumake
            docker
          ];

          shellHook = ''
            export PS1="\[\033[1;32m\][nix:stellar-ingest]\[\033[0m\] \[\033[1;34m\]\w\[\033[0m\] \$ "

            # duckdb-go/v2 bundles a newer DuckDB than the other Obsrvr
            # services, which is why this module stayed outside the
            # ttp-processor-demo go.work. GOWORK=off keeps builds identical
            # whether or not a go.work exists above this directory.
            export GOWORK=off
            export CGO_ENABLED=1

            # A standalone run fails closed if this is inherited from the
            # ambient environment, so the dev shell clears it. Set it
            # deliberately when you actually want control-plane reporting.
            unset ENABLE_FLOWCTL

            echo "stellar-ingest dev shell"
            echo "  go      $(go version | cut -d' ' -f3)"
            echo "  duckdb  $(duckdb --version 2>/dev/null | head -1)"
            echo ""
            echo "  make build-all      stellar-ingest + stellar-history-loader"
            echo "  make test           unit suite"
            echo "  make golden         hermetic Bronze snapshot suite"
            echo "  make golden-update  regenerate goldens (review the diff)"
            echo "  make ci-build       everything CI runs"
          '';
        };

        formatter = pkgs.nixpkgs-fmt;
      }
    );
}
