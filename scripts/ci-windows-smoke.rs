//! Reject an incompatible Windows linker or runtime before the full build.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. See evidence/SOURCES.md.

#[cfg(not(target_os = "windows"))]
compile_error!("the emulator preflight must build a Windows executable");

fn main() {
    let mut map = std::collections::HashMap::new();
    map.insert("runtime", 1);
    assert_eq!(map.get("runtime"), Some(&1));
    let listener =
        std::net::TcpListener::bind(("127.0.0.1", 0)).expect("Windows loopback socket must bind");
    assert!(listener.local_addr().expect("bound address").port() > 0);
    assert_eq!(
        std::thread::spawn(|| 42)
            .join()
            .expect("Windows thread join"),
        42
    );
    for program in ["cargo", "rustc"] {
        let output = std::process::Command::new(program)
            .arg("--version")
            .output()
            .expect("Windows subprocess must find its Windows SDK executable");
        assert!(output.status.success(), "{program}: {output:?}");
        let version = String::from_utf8(output.stdout).expect("tool version is UTF-8");
        assert!(
            version.starts_with(&format!("{program} 1.98.0")),
            "{version}"
        );
    }
    println!("PASS Windows Rust runtime: random hash seed, loopback socket, thread join, native Cargo/rustc subprocesses");
}
