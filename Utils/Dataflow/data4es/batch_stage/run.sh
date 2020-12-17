# $1 has to be used as a workaround to pass empty string as a value for
# -E, since "... -E ''" will treat the single quotes literally due to double
# quotes around them.
cmd="./stage.py -m s -E $1"
cmd_batch2="./stage.py -b 2 -m s -E $1"
cmd_batch100="./stage.py -b 100 -m s -E $1"

# Various tests that should produce the same results.

# Stage chains.
cat inp | $cmd "" | $cmd "" > outp1
cat inp | $cmd_batch2 "" | $cmd_batch2 "" > outp2
cat inp | $cmd_batch100 "" | $cmd_batch100 "" > outp100
cat inp | $cmd "" | $cmd_batch2 "" > outp12
cat inp | $cmd_batch2 "" | $cmd "" > outp21
