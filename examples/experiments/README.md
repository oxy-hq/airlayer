# Experiments (`engine::experiment`)

Runnable Rust examples for the experiment estimator. Unlike the other example directories, there is no CLI here: `engine::experiment` is library-only, so each example is a small `main` that builds a synthetic panel with `PanelMatrix::from_triples` and prints a readable result. Every panel is deterministic (a hashed noise function, fixed seeds), so the output below is stable.

Reference: [docs/experiments.md](../../docs/experiments.md).

| Example | File | Shows |
|---------|------|-------|
| `experiment_common_date` | [common_date.rs](common_date.rs) | Common switch date (Welch): estimate, CI, `pre_trend`, `size_bias`; a misspelled unit refused by name |
| `experiment_staggered` | [staggered.rs](staggered.rs) | `propose_waves` with a holdout; its refusal of a no-holdout plan; a full rollout whose last wave is dropped |
| `experiment_switchback` | [switchback.rs](switchback.rs) | `propose_switchback` + `estimate_switchback`; a 3-pair schedule refused because it cannot reach alpha |
| `experiment_placebo_power` | [placebo_power.rs](placebo_power.rs) | `placebo_power` MDE, unblocked and blocked; refusal when history is too short |
| `experiment_ratio` | [ratio.rs](ratio.rs) | `estimate_ratio` (lever → driver → target): coefficient and Anderson–Rubin set; a soft refusal (weak lever) and a hard one (unit missing) |

```bash
cargo run --example experiment_common_date
cargo run --example experiment_staggered
cargo run --example experiment_switchback
cargo run --example experiment_placebo_power
cargo run --example experiment_ratio
```

Each finishes in about a second in a debug build (`placebo_power` uses 200 iterations to stay quick).

## Refusals are values

Every example shows at least one refusal. A refused `EffectResult`, `PowerResult` or `RatioResult` carries `refusal: Some(reason)` with every number `NaN`. Nothing panics and nothing returns `Err`, so always check `refusal` before reading the numbers. `estimate_ratio` also has *soft* refusals: the coefficient and set are kept, but `refusal` explains why they should not be trusted. `propose_waves` is the exception: it is a planner, and returns `Result<_, String>`.
