#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Schema.h"
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"

#include <string>
#include <vector>
#include <map>

using namespace std;

struct mapkey{
    string tableName;
    string key;
};
struct opt{
    unsigned long long size;
    unsigned long long cost;
    Schema schema;
    string key;
    bool singleTable;
};
// data structure used by the optimizer to compute join ordering
struct OptimizationTree {
	// list of tables joined up to this node
	vector<string> tables;
	// number of tuples in each of the tables (after selection predicates)
	vector<int> tuples;
	// number of tuples at this node
	int noTuples;

	// connections to children and parent
	OptimizationTree* parent;
	OptimizationTree* leftChild;
	OptimizationTree* rightChild;
};

class QueryOptimizer {
private:
	Catalog* catalog;
        map<string,opt> initial;
        map<string,string> final;
        vector<mapkey> mapKey;

public:
	QueryOptimizer(Catalog& _catalog);
	virtual ~QueryOptimizer();

	void Optimize(TableList* _tables, AndList* _predicate, OptimizationTree* _root);
        void InitializeMaps(TableList* _tables);
        void CalculateCosts(TableList* _tables, AndList* _predicate);
        void CalculateSize(TableList* _tables, AndList* _predicate);
        void PrintTables();
        void RegenerateTree(string _tableIndecies, OptimizationTree* & root);
        void RegenerateLeaf(string _tableIndecies, OptimizationTree* & root);
        void Partition(string _tableIndecies);
        void PrintOptimizationTree(OptimizationTree* & root);
        /* Computes permutations
         * Use output as your return value
         * pass bin.size(0 as size_bin
         */
        void Permute(vector<vector<string> >& output,vector<string> bin,int size_bin);
        bool Permute(string& array);
        int Factorial(int n);
};

#endif // _QUERY_OPTIMIZER_H
