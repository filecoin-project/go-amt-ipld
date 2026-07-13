# Fuzzer to validate the AMT

Uses native Go fuzzing. Random byte input is decoded as a sequence of AMT
operations (set, get, delete, flush, reload), mirrored into a plain map, and
the AMT is checked against the map by iteration, random gets, and root CID
reconstruction.

To fuzz:

    go test -fuzz=FuzzAMTOps .

Seed corpus lives in `testdata/fuzz/FuzzAMTOps/` and is replayed by plain
`go test`; commit minimised failing inputs there as regression tests. To
trace ops while reproducing a failure, set `debug = true` in
`checked_amt_test.go` and run with `-v`.
