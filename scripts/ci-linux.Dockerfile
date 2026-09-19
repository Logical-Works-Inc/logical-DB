# Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
# S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. See evidence/SOURCES.md.
# Existing self-hosted runner image; deliberately no hosted execution service.
FROM logical-db-act:local
RUN apt-get update && apt-get install --no-install-recommends -y libclang-dev
RUN rustup toolchain install 1.98.0 --profile minimal --component clippy,rustfmt
ENV CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 RUST_BACKTRACE=1
# Keep the same link configuration in default and FDB checks. Otherwise Cargo
# rebuilds every shared dependency when the optional lane introduces RUSTFLAGS.
ENV RUSTFLAGS="-L native=/opt/logical-db-fdb-7.3.77 -C link-arg=-Wl,-rpath,/opt/logical-db-fdb-7.3.77"
WORKDIR /workspace
