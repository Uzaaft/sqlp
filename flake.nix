{
  description = "sqlp - Async Python ORM with Pydantic integration";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            python313
            uv
            postgresql
            mysql
            sqlite
            git
            jj
          ];

          shellHook = ''
            echo "sqlp devshell loaded"
            export UV_PYTHON=${pkgs.python313}/bin/python3.13
          '';
        };
      }
    );
}
