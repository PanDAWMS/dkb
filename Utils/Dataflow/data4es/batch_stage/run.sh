source ../../shell_lib/eop_filter

cmd="./stage.py -m s"
cmd_batch2="./stage.py -b 2 -m s"
cmd_batch100="./stage.py -b 100 -m s"

# Various tests that should produce the same results.

# Stage chains.
# these differ by size without eop_filter at the end
cat inp | $cmd | eop_filter | $cmd | eop_filter > outp1
cat inp | $cmd_batch2 | eop_filter | $cmd_batch2 | eop_filter > outp2
cat inp | $cmd_batch100 | eop_filter | $cmd_batch100 | eop_filter > outp100
cat inp | $cmd | eop_filter | $cmd_batch2 | eop_filter > outp12
cat inp | $cmd_batch2 | eop_filter | $cmd | eop_filter > outp21
