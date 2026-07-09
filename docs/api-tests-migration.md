# HDF5 API test suite migration

`test/` no longer depends on the `HDFGroup/vol-tests` submodule. CI instead
builds HDF5 from source as the top-level project and pulls this connector
in as an `HDF5_VOL_ALLOW_EXTERNAL` subproject, the same mechanism HDF5's own
CI uses to test other external VOL connectors (see `vol_async.yml`,
`vol_cache.yml`, and `vol_rest.yml` in `HDFGroup/hdf5`'s
`.github/workflows/`). HDF5's own `test/API`/`testpar/API` suite then
registers this connector's ctest entries automatically.

## How it's wired

CI configures HDF5 with:

```
-DHDF5_VOL_ALLOW_EXTERNAL=LOCAL_DIR
-DHDF5_VOL_PATH01=<path to this checkout>
-DHDF5_VOL_VOL-DAOS_NAME=daos
-DHDF5_VOL_VOL-DAOS_TEST_PARALLEL=ON
```

HDF5's `CMakeVOL.cmake` `add_subdirectory()`s this repo directly into its
own build, auto-stripping this project's `find_package(HDF5 ...)` calls
(they'd conflict with targets HDF5's own in-progress build is generating)
and pre-setting `HDF5_FOUND`/`HDF5_LIBRARIES`/etc. for this project to
consume instead. `test/API/CMakeLists.txt` then registers one
`HDF5_VOL_vol-daos-h5_api_test_<iface>` ctest entry per API test interface,
plus `HDF5_VOL_vol-daos-h5_api_ext_test_<name>` entries for this
connector's own native tests (`test/daos_vol`'s `h5daos_test_*` binaries,
exposed via the `HDF5_API_EXT_SERIAL_TESTS`/`HDF5_API_EXT_PARALLEL_TESTS`
variables `test/daos_vol/CMakeLists.txt` sets).

This project does not run its own test-orchestration driver. The DAOS
server, agent, and pool are started once per CI job via plain shell steps
(`ci.yml`), not per test -- the same pattern HDF5's own CI uses to start
HSDS once for `vol-rest`. `test/CMakeLists.txt` still renders
`daos_server.yml`/`daos_agent.sh`/`daos_pool.sh` from their `.in` templates
via `configure_file()`; CI locates and runs them directly.

## Handling unsupported features

Where the test suite checks `H5VL_CAP_FLAG_*` before exercising a feature,
no exclusion is needed -- the connector's capability query already tells
the test to skip it. Gaps with no capability flag to gate on are excluded
at the ctest level in `ci.yml` (see "Known gaps" below); HDF5's own driver
mechanism for finer-grained per-subtest exclusion (`-x <subtest>`) is not
used here since this project doesn't run through it.

## Known gaps

`ci.yml`'s `ctest` invocation excludes the following as known, tracked
connector gaps rather than regressions:

- `HDF5_VOL_vol-daos-h5_api_test_attribute` -- decreasing-order
  (`H5_ITER_DEC`) attribute iteration is unsupported (`src/daos_vol_attr.c`).
- `HDF5_VOL_vol-daos-h5_api_test_testhdf5` -- its `tattr.c` subtest hits the
  same by-idx/paginated attribute lookup gap while iterating hundreds of
  attributes. Manifests non-deterministically: sometimes each lookup fails
  fast and the binary finishes in ~20s, other times the cascading
  diagnostic output from hundreds of failing lookups is enough to blow
  past the ctest timeout instead.
- `HDF5_VOL_vol-daos-h5_api_test_parallel_t_bigio` -- fails with
  `DER_NOSPACE`; the 4GB test pool (`DAOS_POOL_SIZE`) is too small for this
  test's writes.
- `HDF5_VOL_vol-daos-h5_api_test_parallel_testphdf5` -- its `h5oflusherror`
  subtest expects `H5Oflush` to fail (a native-VOL-only limitation); DAOS
  correctly succeeds, but there's no per-subtest exclusion available
  without HDF5's own C++ test-API driver, so the whole binary is excluded.

Beyond the core API test suite, `-R 'HDF5_VOL_vol-daos'` also matches
HDF5's own tool-test suites (`tools/test/{h5dump,h5diff,h5copy,h5ls}`,
which likewise iterate `HDF5_EXTERNAL_VOL_TARGETS`). On the `develop`
matrix leg this currently surfaces a real connector gap, not a test
harness bug: `h5dumpgentest` (the fixture that generates reference files
for nearly all `H5DUMP` comparison tests) crashes because DAOS VOL
correctly reports external-link creation, UD-link creation, and
`H5Oset_comment`/object-optional as unsupported, and `h5dumpgentest`'s own
generator code (written for the native VOL) doesn't check those return
codes before using the resulting handle. Since `h5dumpgentest` is a single
shared fixture, this one crash cascades into most of the tool-test
failures on that leg. The real fix is adding external-link, UD-link, and
`H5Oset_comment` support to `src/daos_vol_link.c`/`src/daos_vol_obj.c` --
tracked as a separate connector-feature gap, not addressed by this PR.
