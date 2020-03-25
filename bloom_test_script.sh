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

if false;then
run 4 1 4000
run 4 2 4000
run 4 3 4000
run 4 4 4000
run 4 5 4000
run 4 6 4000
fi

if false;then
run 4 1 100000000
run 4 2 100000000
run 4 3 100000000
run 4 4 100000000
run 4 5 100000000
run 4 6 100000000
fi

#run 4 1 200000000
#run 4 2 100000000
