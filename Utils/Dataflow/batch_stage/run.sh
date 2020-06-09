source ../shell_lib/eop_filter

cmd="./stage.py -m s"
cmd_batch2="./stage.py -m s -b 2"
cmd_batch100="./stage.py -m s -b 100"

# Various tests that should produce the same results.

# Stage chains.
# these differ by size without eop_filter at the end
cat inp | $cmd | eop_filter | $cmd | eop_filter > outp1
cat inp | $cmd_batch2 | eop_filter | $cmd_batch2 | eop_filter > outp2
cat inp | $cmd_batch100 | eop_filter | $cmd_batch100 | eop_filter > outp100
cat inp | $cmd | eop_filter | $cmd_batch2 | eop_filter > outp12
cat inp | $cmd_batch2 | eop_filter | $cmd | eop_filter > outp21

# Input where some messages are "bad" and should be discarded.
cat discard_inp | $cmd | eop_filter > outp_discard
cat discard_inp | $cmd_batch2 | eop_filter > outp_discard2
cat discard_inp | $cmd_batch100 | eop_filter > outp_discard100
