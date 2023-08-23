{ pkgs ? import <nixpkgs> { } }:
pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    cargo
    cargo-watch
    cargo-nextest
    rustc
    rustfmt
    clippy
    pkg-config
    sqlite
  ];
  packages = with pkgs; [ bacon ];
  RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
}
