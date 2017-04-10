#!/bin/bash
# My script to test queries. Out put goes to txt files in folder.

echo Running Tests...

#make clean
#make

for i in {1..10}
do
	echo "Query $i: queryOut$i.txt"
	./main.out < Queries/$i.sql > queryOut$i.txt
done

echo Done!! opening in mousepad

mousepad queryOut*.txt

exit 