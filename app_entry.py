"""Entry point used by PyInstaller (build_exe.bat) - opens the GUI."""
import sys

from tdata.__main__ import main

if __name__ == "__main__":
    sys.exit(main(["gui"]))
