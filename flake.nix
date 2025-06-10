{
  description = "Pure-Go DuckDB driver development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        
        # Go version
        go = pkgs.go_1_21;
        
        # Development tools
        devTools = with pkgs; [
          # Go tools
          go
          gopls
          go-tools
          golangci-lint
          delve
          gomodifytags
          gotests
          impl
          
          # DuckDB
          duckdb
          
          # Build tools
          gnumake
          gcc
          pkg-config
          
          # Utilities
          git
          jq
          ripgrep
          fd
          bat
        ];

        # Shell hook to set up environment
        shellHook = ''
          echo "🦆 DuckDB Pure-Go Driver Development Environment"
          echo ""
          echo "DuckDB version: $(duckdb --version | head -n1)"
          echo "Go version: $(go version)"
          echo ""
          echo "DuckDB library location:"
          echo "  $(find ${pkgs.duckdb}/lib -name "libduckdb.*" | head -n1)"
          echo ""
          echo "Available commands:"
          echo "  make test      - Run all tests"
          echo "  make bench     - Run benchmarks"
          echo "  make coverage  - Generate coverage report"
          echo "  make lint      - Run linters"
          echo "  make run-basic - Run basic example"
          echo "  make run-adv   - Run advanced example"
          echo ""
          
          # Set library path for purego
          export LD_LIBRARY_PATH="${pkgs.duckdb}/lib:$LD_LIBRARY_PATH"
          export DYLD_LIBRARY_PATH="${pkgs.duckdb}/lib:$DYLD_LIBRARY_PATH"
          
          # For debugging
          export CGO_ENABLED=0
          export DUCKDB_LIB_DIR="${pkgs.duckdb}/lib"
        '';

      in
      {
        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = devTools;
          inherit shellHook;
        };

        # Package definition
        packages.default = pkgs.buildGoModule {
          pname = "dukdb-go";
          version = "0.1.0";
          
          src = ./.;
          
          vendorHash = null; # Will be set after running go mod vendor
          
          # Ensure DuckDB is available at runtime
          buildInputs = [ pkgs.duckdb ];
          
          # Disable CGO
          CGO_ENABLED = 0;
          
          # Set library paths
          preBuild = ''
            export LD_LIBRARY_PATH="${pkgs.duckdb}/lib:$LD_LIBRARY_PATH"
            export DYLD_LIBRARY_PATH="${pkgs.duckdb}/lib:$DYLD_LIBRARY_PATH"
          '';
          
          # Don't run tests during build (they need DuckDB library)
          doCheck = false;
          
          meta = with pkgs.lib; {
            description = "Pure-Go DuckDB driver";
            homepage = "https://github.com/connerohnesorge/dukdb-go";
            license = licenses.gpl3;
            maintainers = [ ];
          };
        };

        # Test runner that ensures DuckDB is available
        apps.test = flake-utils.lib.mkApp {
          drv = pkgs.writeShellScriptBin "test-dukdb-go" ''
            export LD_LIBRARY_PATH="${pkgs.duckdb}/lib:$LD_LIBRARY_PATH"
            export DYLD_LIBRARY_PATH="${pkgs.duckdb}/lib:$DYLD_LIBRARY_PATH"
            export CGO_ENABLED=0
            
            echo "Running tests with DuckDB ${pkgs.duckdb.version}..."
            ${go}/bin/go test -v ./...
          '';
        };

        # Benchmark runner
        apps.bench = flake-utils.lib.mkApp {
          drv = pkgs.writeShellScriptBin "bench-dukdb-go" ''
            export LD_LIBRARY_PATH="${pkgs.duckdb}/lib:$LD_LIBRARY_PATH"
            export DYLD_LIBRARY_PATH="${pkgs.duckdb}/lib:$DYLD_LIBRARY_PATH"
            export CGO_ENABLED=0
            
            echo "Running benchmarks with DuckDB ${pkgs.duckdb.version}..."
            ${go}/bin/go test -bench=. -benchmem ./test/...
          '';
        };

        # Example runners
        apps.example-basic = flake-utils.lib.mkApp {
          drv = pkgs.writeShellScriptBin "example-basic" ''
            export LD_LIBRARY_PATH="${pkgs.duckdb}/lib:$LD_LIBRARY_PATH"
            export DYLD_LIBRARY_PATH="${pkgs.duckdb}/lib:$DYLD_LIBRARY_PATH"
            export CGO_ENABLED=0
            
            ${go}/bin/go run examples/basic.go
          '';
        };

        apps.example-advanced = flake-utils.lib.mkApp {
          drv = pkgs.writeShellScriptBin "example-advanced" ''
            export LD_LIBRARY_PATH="${pkgs.duckdb}/lib:$LD_LIBRARY_PATH"
            export DYLD_LIBRARY_PATH="${pkgs.duckdb}/lib:$DYLD_LIBRARY_PATH"
            export CGO_ENABLED=0
            
            ${go}/bin/go run examples/advanced.go
          '';
        };
      });
}