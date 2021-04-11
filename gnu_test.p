set terminal png
set output "plots/small_low_cont.png"
set style data histogram
set style histogram cluster gap 1
set style fill solid border rgb "black"
set auto x
set yrange [0:*]
set key outside
set key width -3
set title "Small Txn (5 records) Performance under low contention"
set ylabel "throughput (txn/sec)"
plot "small_low_cont.data" using 2:xtic(1) title col, \
        '' using 3:xtic(1) title col, \
        '' using 4:xtic(1) title col, \
        '' using 5:xtic(1) title col, \
        '' using 6:xtic(1) title col