set terminal png
set output "plots/res_param.png"
set style data linespoints
set key samplen 2 font ",10"
set key outside
set rmargin 12
set xlabel "alpha"
set ylabel "throughput (txn/sec)"
plot 'res_param.data' using 2:xtic(1) title columnhead(2), '' using 3:xtic(1) title columnhead(3), '' using 4:xtic(1) title columnhead(4),\
        '' using 5:xtic(1) title columnhead(5), '' using 6:xtic(1) title columnhead(6)










#set style line 1 \
#    linecolor rgb '#0060ad' \
#    linetype 1 linewidth 2 \
#    pointtype 7 pointsize 1.5
#set style line 2 \
#    linecolor rgb '#dd181f' \
#    linetype 1 linewidth 2 \
#    pointtype 7 pointsize 1.5
#plot "data.dat" using 1:2 index 0 title "part 1" with linespoints linestyle 1,\
#    ""          using 1:2 index 1 title "part 2" with linespoints linestyle 2