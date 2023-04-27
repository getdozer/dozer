# dozer-log-python

This is the Python binding for reading Dozer logs.

## Installation

`pip install pydozer_log`

## Usage

Assume your Dozer home directory is `.dozer` and you have an endpoint named `trips`. You can read the Dozer logs in Python as follows:

```python
import pydozer_log

reader = await pydozer_log.LogReader.new('.dozer', 'trips')
print(await reader.next_op())
```

## Develop

Install `maturin` in your Python environment:

```bash
pip install maturin
```

Install the development version of `dozer-log-python`:

```bash
maturin develop --features python-extension-module
```

The whole library is behind a feature flag, so you need to specify `--features python-extension-module` to enable it.
This is a [known issue](https://pyo3.rs/v0.18.3/faq.html#i-cant-run-cargo-test-or-i-cant-build-in-a-cargo-workspace-im-having-linker-issues-like-symbol-not-found-or-undefined-reference-to-_pyexc_systemerror) of `pyo3`.

See [PyO3](https://pyo3.rs) for more information.

## Run example

```bash
python examples/reader.py
```
