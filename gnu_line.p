set terminal png
set output "test.png"
set style line 1 \
    linecolor rgb '#0060ad' \
    linetype 1 linewidth 2 \
    pointtype 7 pointsize 1.5
set style line 2 \
    linecolor rgb '#dd181f' \
    linetype 1 linewidth 2 \
    pointtype 7 pointsize 1.5
plot "data.dat" using 1:2 index 0 title "part 1" with linespoints linestyle 1,\
    ""          using 1:2 index 1 title "part 2" with linespoints linestyle 2