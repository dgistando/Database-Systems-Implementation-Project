#include <string>
#include <vector>
#include <iostream>
#include <algorithm>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
	catalog = &_catalog;
}

QueryOptimizer::~QueryOptimizer() {

}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {
	cout<<"Optimize called"<<endl;

	
	// compute the optimal join order
	//preprocessing 2
	//calculate the best way to join the tables. aka matrix chain ordering
	//get dimensions
	//run matrix chain
	//rebuild tableList

}
