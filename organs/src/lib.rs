//! ldb-organs: the donor-engine dump. Every engine named in
//! docs/FRANKENSTEIN.md compiles and runs here with a functional smoke
//! test BEFORE its ideas are re-implemented inside ldb-kernel. This is
//! the receipt that each organ works on this platform; the cleanup pass
//! deletes whichever crates we end up not grafting. Nothing here is on
//! the kernel's critical path.

// ── smoke modules ────────────────────────────────────────────

pub mod dump;

#[cfg(test)]
mod tests {
    // One integration-shaped assertion per donor, kept in the modules.
}
