#include <string>
#include <vector>
#include <iostream>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	// compute the optimal join order
	/**
	Input
	1. table names and total number of tables NT
	2. selection predicates
	3. join predicates

	Data structures
	1. Map : map <table_name> -> tuple (size, cost, order)
	-- table_name : a name that puts all the table names together, when there are more
	-- size : the cardinality, i.e., number of tuples, in table(s)
	-- cost : the sum of intermediate number of tuples in the tree (query optimization cost)
	-- order : join order with groupings of tables explicit

	Pre-processing
	1. fill the map for every individual table name
	-- Map<(T)> = (size from catalog, 0, (T))
	2. push-down selections: update the size for each table based on the estimation of
	   number of tuples following the application of selection predicates
	-- Map<(T)> = (size after push-down selection, 0, (T))
	3. fill the map for every pair of two tables (T1,T2)
	-- sort ascending T1, T2 (no need to have entries in Map for (T1,T2) and (T2,T1)
	   because of commutativity)
	-- estimate cardinality of join using distinct values from Catalog and join predicates
	-- Map<(T1,T2)> = (Map<(T1)>.size*Map<(T2)>.size/selectivity, 0, (T1,T2))

	Algorithm
	- consider all the permutations at the top and build all possible tree shapes for
	  every permutation
	- use and update Map as the algorithm progresses

	Partition (N, array[tables])
	- build all possible tree shapes by repeated partitioning
	1. if (Map<sort-asc(T1,T2,...,Tn)> is not empty) return
	2. for i = 1 to N! do
			 Let (T1',T2',...,Tn') be the ith permutation of N tables. You can find
			 algorithms on how to generate this permutation on Wikipedia: Permutation.
	3.   for j = 1 to N-1 do
				 left = (T1',...,Tj')
				 if (Map<sort-asc(left)> is empty) Partition (j, left)
				 right = (T(j+1)',...,Tn')
				 if (Map<sort-asc(right)> is empty) Partition (N-j, right)

	4.		 // at this point we are guaranteed to have entries in the Map on left and right
				 // compute the cost for this tree and store, if minimum; otherwise, discard
				 cost = Map<sort-asc(left)>.cost + Map<sort-asc(right)>.cost
				 if (j!=1) cost += Map<sort-asc(left)>.size
				 if (j!=N-1) cost += Map<sort-asc(right)>.size
				 if (cost < min_cost)
					 // estimate cardinality of join using distinct values from Catalog and join predicates
				   size = Map<(left)>.size*Map<(right)>.size/selectivity
				 	 order = (Map<sort-asc(left)>.order,Map<sort-asc(right)>.order)
					 min_cost = cost
	5. Map<sort-asc(T1,T2,...,Tn)> = (size,min_cost,order)
	*/
}
