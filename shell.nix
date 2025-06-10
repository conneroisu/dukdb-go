# Compatibility wrapper for nix-shell users
{ pkgs ? import <nixpkgs> {} }:

let
  flake = builtins.getFlake (toString ./.);
  system = pkgs.system;
in
  flake.devShells.${system}.default