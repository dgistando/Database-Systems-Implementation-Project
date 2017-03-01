#include <string>
#include <vector>
#include <iostream>

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
//        if(_root->tables.size() != 0){ return; }
//        vector<vector<string> > permutations;
//        Permute(permutations,_root->tables,_root->tables.size());
//        for(int i = 0; i < Factorial(_root->tables.size()); i++){
//            for(int j = 0; j < permutations.at(i).size(); j++){
//                _root->leftChild = new OptimizationTree();
//                //_root->leftChild.
//                //if(_root->leftChild is empty ) { Optimize();}
//                _root->rightChild = new OptimizationTree();
//                //if(_root->rightChild is empty ) { Optimize();}
//                //int cost = _root->leftChild->noTuples + _root->rightChild->noTuples;
//                //if(j != 1) { cost += }
//            }
//        } 
        
	
}

int QueryOptimizer::Factorial(int n){if(n < 0) { return 0; } return !n ? 1 : n * Factorial(n - 1); }
void QueryOptimizer::Permute(vector<vector<string> >& output, vector<string> bin,int size_bin){
    if(size_bin == 1){ output.push_back(bin); return; }
    for(int i = 0; i < size_bin - 1; i ++){
        Permute(output,bin,size_bin -1);
        if(size_bin % 2 == 1) { swap(bin.at(0),bin.at(size_bin-1)); }
        else { swap(bin.at(i),bin.at(size_bin-1)); }
    }
}
