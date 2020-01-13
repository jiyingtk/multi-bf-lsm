OUTFILE=out.txt
BINFILE=./out-static/bloom_test

echo output file $OUTFILE

run(){
    echo ---------- >> $OUTFILE
    echo filter num: $2, per filter bits: $1, test key nums: $3 >> $OUTFILE
    $BINFILE $1 $2 $3 2>&1 | grep "\[Perf\]" >> $OUTFILE
    echo ""  >> $OUTFILE
    echo ""  >> $OUTFILE
}


run 4 1 2000
run 4 2 2000
run 4 3 2000
run 4 4 2000
run 4 5 2000
run 4 6 2000