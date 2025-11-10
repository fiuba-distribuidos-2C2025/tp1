{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # Python
    python311
    python311Packages.pip
    python311Packages.virtualenv

    # Go
    go
    
    # Optional: useful tools
    git
  ];

  shellHook = ''
    echo "Development environment loaded!"
    echo "Python version: $(python --version)"
    echo "Go version: $(go version)"
    echo ""
    echo "Available commands:"
    echo "  python - Python interpreter"
    echo "  pip - Python package installer"
    echo "  go - Go compiler and tools"
  '';
}
