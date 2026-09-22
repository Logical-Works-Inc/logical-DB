#!/usr/bin/env bash
# Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
# S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. See evidence/SOURCES.md.
set -euo pipefail
# Windows tests spawn Cargo for the real dependency gate. Give Wine actual
# Windows executables; do not substitute cached or synthetic metadata.
sdk=/root/.rustup/toolchains/1.98.1-x86_64-pc-windows-gnu/bin
pthread_dll=$(x86_64-w64-mingw32-gcc-posix -print-file-name=libwinpthread-1.dll)
cpp_dll=$(x86_64-w64-mingw32-g++-posix -print-file-name=libstdc++-6.dll)
WINEPATH="$sdk;$(dirname "$pthread_dll");$(dirname "$cpp_dll")"
export WINEPATH
exec wine "$@"
