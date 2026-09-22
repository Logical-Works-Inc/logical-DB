# Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
# S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. See evidence/SOURCES.md.
FROM logical-db-act:local AS toolchain
FROM ghcr.io/cross-rs/x86_64-pc-windows-gnu@sha256:7dbfe2c497a798ee3798fecf4b262c0ea12d1d02a6331bdc05ce76c9ad90fd70
COPY --from=toolchain /root/.cargo/bin/ /root/.cargo/bin/
ENV PATH=/root/.cargo/bin:$PATH
RUN rustup toolchain install 1.98.1 --profile minimal --component clippy,rustfmt --target x86_64-pc-windows-gnu
RUN rustup toolchain install 1.98.1-x86_64-pc-windows-gnu --profile minimal --force-non-host --no-self-update
RUN apt-get update && apt-get install -y --no-install-recommends python3
ENV CARGO_BUILD_TARGET=x86_64-pc-windows-gnu CARGO_BUILD_JOBS=4 CARGO_INCREMENTAL=0 RUST_BACKTRACE=1 WINEDEBUG=-fixme
# This selects a fresh Cargo fingerprint after the incompatible old MinGW
# image, so old native objects cannot masquerade as a new-toolchain build.
ENV RUSTFLAGS="-C linker=/usr/bin/x86_64-w64-mingw32-gcc-posix"
ENV CARGO_TARGET_X86_64_PC_WINDOWS_GNU_RUNNER=/usr/local/bin/logical-db-wine RUST_TEST_THREADS=4
COPY scripts/ci-wine-runner.sh /usr/local/bin/logical-db-wine
RUN chmod 0755 /usr/local/bin/logical-db-wine
COPY scripts/ci-windows-smoke.rs /tmp/ci-windows-smoke.rs
RUN x86_64-w64-mingw32-gcc-posix --version && wine --version \
    && rustc /tmp/ci-windows-smoke.rs --target x86_64-pc-windows-gnu -C linker=/usr/bin/x86_64-w64-mingw32-gcc-posix -o /tmp/ci-windows-smoke.exe \
    && timeout --signal=TERM --kill-after=2s 60 /usr/local/bin/logical-db-wine /tmp/ci-windows-smoke.exe
WORKDIR /workspace
