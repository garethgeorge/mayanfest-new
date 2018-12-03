#!/bin/bash
mountpoint="../build/mount/"
filename="hello"
filename="$mountpoint$filename"
maxnumbytes=33554432 #32*1024*1024
numbytes=1024

rm stats

while [ $numbytes -le $maxnumbytes ]
do
	#mount
	./run.sh &
	while true
	do
		if [ `stat -c%d "$mountpoint"` != `stat -c%d "$mountpoint/.."` ]; then
			break
		fi
	done
	echo "done mounting"

	touch $filename
	starttime=`date +%s%N` #nanoseconds
	for ((i=1; i<=$numbytes; i=i+1))
	do
		echo 'a' >> $filename
	done
	endtime=`date +%s%N` #nanoseconds
	runtime=$((endtime-starttime))
	throughput=`echo "scale=6; ($numbytes * 2) / ($runtime / 1000000000) " | bc -l`

	echo $throughput >> stats

	numbytes=$((numbytes*2))

	$unmount
	umount $mountpoint
done



