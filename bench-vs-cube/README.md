# bench-vs-cube

Compares airlayer SQL compilation speed against Cube.js.

## Run

```bash
cd bench-vs-cube && docker compose up -d && sleep 15 && cargo run --release
```

Optionally pass iteration count:

```bash
cargo run --release -- 1000
```

## Teardown

```bash
docker compose down
```
