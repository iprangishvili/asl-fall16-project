#!/bin/bash
rm -rf Result
mkdir Result
cd ~/libmemcached-1.0.18/clients

number=5
maxclients=16
keyword="clients"
for num in $(seq 1 $number); 
do
	for count in $(seq 1 $maxclients)
	do	
		numclients=$(( 4*$count ))		
		echo "Iteration: $numclients"
		./memaslap -s 10.0.0.4:11212 -T $numclients -c $numclients -o0.9 -t 30s -S 30s> ~/Result/result$num$keyword$numclients.txt
	done    
done
