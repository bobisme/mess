fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cmd = clap::Command::new("xtask")
        .bin_name("xtask")
        .subcommand_required(true)
        .subcommand(
            clap::command!("coverage")
                .arg(
                    clap::arg!(--"output-dir" <PATH>)
                        .value_parser(clap::value_parser!(std::path::PathBuf))
                        .default_value("."),
                )
                .arg(clap::arg!(--"html")),
        );
    let matches = cmd.get_matches();
    match matches.subcommand() {
        Some(("coverage", matches)) => coverage(matches)?,
        _ => unreachable!("clap should ensure we don't get here"),
    };
    Ok(())
}

fn rm_glob(pattern: &str) -> Result<(), Box<dyn std::error::Error>> {
    for entry in glob::glob(pattern)? {
        let path = entry?;
        std::fs::remove_file(&path).unwrap_or_else(|err| {
            eprintln!(
                "ERROR: couldn't remove file {}: {:?}",
                path.display(),
                err
            );
        });
    }
    Ok(())
}

fn coverage(args: &clap::ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    let output_dir = args.get_one::<std::path::PathBuf>("output-dir").unwrap();
    if !output_dir.exists() {
        println!(
            "INFO: directory {} does not exist, creating",
            output_dir.display()
        );
        std::fs::create_dir_all(output_dir)?;
    }

    rm_glob("target/**/*.gc{da,no}")?;

    println!("=== running coverage ===");
    duct::cmd!("cargo", "test")
        .env("RUSTC_BOOTSTRAP", "1")
        .env("CARGO_INCREMENTAL", "0")
        .env("LLVM_PROFILE_FILE", "cargo-test-%p-%m.profraw")
        .env("RUSTDOCFLAGS", "-Cpanic=abort")
        .env(
            "RUSTFLAGS",
            "-Zprofile -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code -Coverflow-checks=off -Zpanic_abort_tests -Cpanic=abort",
        )
        .run()?;
    println!("ok.");

    println!("=== generating report ===");
    let (fmt, file) = if args.get_flag("html") {
        ("html", output_dir.join("html"))
    } else {
        ("lcov", output_dir.join("lcov.info"))
    };
    if file.exists() && fmt == "lcov" {
        std::fs::remove_file(&file)?;
    }

    duct::cmd!(
        "grcov",
        ".",
        "--binary-path",
        "./target/debug/deps",
        "-s",
        ".",
        "-t",
        fmt,
        "--branch",
        "--ignore-not-existing",
        "--ignore",
        "../*",
        "--ignore",
        "/*",
        "--ignore",
        "xtask/*",
        "--ignore",
        "*/src/tests/*",
        "-o",
        file,
    )
    .run()?;
    println!("ok.");

    println!("=== cleaning up ===");
    // rm_glob("*.profraw")?;
    println!("ok.");

    Ok(())
}
