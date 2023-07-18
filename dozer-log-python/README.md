# dozer-log-python

This is the Python binding for reading Dozer logs.

## Installation

`pip install pydozer_log`

### Troubleshoot

This library contains native code so have limited platform support.

If above command gives you:

```text
ERROR: Could not find a version that satisfies the requirement pydozer_log (from versions: none)
ERROR: No matching distribution found for pydozer_log
```

it means your platform is not supported.

As a general rule, we support CPython >= 3.10 on Windows, MacOS and Linux, both amd and arm architectures. Meanwhile, some other versions of PyPy and CPython with certain platform combinations are also supported.

To see all supported platforms, please check the file list at <https://pypi.org/project/pydozer-log/#files>.

## Usage

Assume your have Dozer running and the app is listening to `http://127.0.0.1:50053`, and you have an endpoint named `trips`. You can read the Dozer logs in Python as follows:

```python
import pydozer_log

reader = await pydozer_log.LogReader.new('http://127.0.0.1:50053', 'trips')
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

From the repository root:

```bash
python examples/reader.py
```
