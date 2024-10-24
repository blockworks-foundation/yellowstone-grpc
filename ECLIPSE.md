## About
This fork of yellowstone plugin was adapted to work with eclipse validator.
See also notions document.


## Patches
(solar validator 1.17.6, rust 1.73)

```bash
# look through this dependencies
cargo tree | grep solana | grep -v solar
```


```bash
cargo update -p clap --precise 4.4.18

# should fix:
# error: package `clap_builder v4.5.20` cannot be built because it requires rustc 1.74 or newer, while the currently active rustc version is 1.73.0
```
