set terminal png
set output "plots/tpcc.png"
set style data histogram
set style histogram cluster gap 2.5
set xtics font ", 10"
set key samplen 2 font ",10"
set style fill solid border rgb "black"
set auto x
set yrange [0:*]
set key width -3
set ylabel "throughput (txn/sec)"
plot "tpcc.data" using 2:xtic(1) title col, \
        '' using 3:xtic(1) title col, \
        '' using 4:xtic(1) title col, \
        '' using 5:xtic(1) title col, \
        '' using 6:xtic(1) title col