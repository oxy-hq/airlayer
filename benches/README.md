# Benchmarks

## airlayer compilation (`cargo bench`)

```bash
cargo bench
```

Criterion benchmark measuring pure in-process SQL compilation time. HTML reports in `target/criterion/`.

## airlayer vs Cube (`bench-vs-cube/`)

Separate binary that compares airlayer against Cube.js. See [`bench-vs-cube/README.md`](../bench-vs-cube/README.md).

```bash
cd bench-vs-cube && docker compose up -d && sleep 15 && cargo run --release
```
