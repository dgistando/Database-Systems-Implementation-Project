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
        map<string,opt> tableOptMap;
        map<string,string> tableNamesMap;
        vector<mapkey> mapKey;

public:
	QueryOptimizer(Catalog& _catalog);
	virtual ~QueryOptimizer();

	void Optimize(TableList* _tables, AndList* _predicate, OptimizationTree*& _root);
        /* Initializes mapKey vector, tableOpt map, tableNames map.
         * mapKey stores tableName relation to the key in tableOpt and tableNames map
         * tableOpt maps original and generated table permutation keys to an Opt structure
         * tableNames maps key to to original tables
         */
        void InitializeMaps(TableList* _tables);
        /* Calculates the size for each table given by the TableList
         * Does push down selections 
         */
        void CalculateSizeForOriginalTables(TableList* _tables, AndList* _predicate);
        /* Calculates size for combination of tables using (#Tuples1 * #Tuples2)/ (Max(#Distinct1,#distinct2))
         * iterates tru TableList and combines with other tables.
         */
        void CalculateSizeForTableCombination(TableList* _tables, AndList* _predicate);
        /* Prints the table nam, cost, and key
         * Changes over each step
         */
        void PrintTables();
        /* Iterates tru the tree and prints out tables stored in leafs
         */
        void PrintOptimizationTree(OptimizationTree* & _root);
        /* Unfinished: calls RegenerateLeaf()
         */
        void RegenerateTree(string _tableIndecies, OptimizationTree* & _root);
        /* Rebuild the tree leafs using tableOpt map to get size and string to get the key for name
         */
        void RegenerateLeaf(string _tableIndecies, OptimizationTree* & _root);
        /* partition algo
         */
        void Partition(string _tableIndecies);
        /* Computes permutations
         * Use output as your return value
         * pass bin.size(0 as size_bin
         */
        void Permute(vector<vector<string> >& output,vector<string> bin,int size_bin);
        /* std: :string permutation algo
         */
        bool Permute(string& array);
        int Factorial(int n);
};

#endif // _QUERY_OPTIMIZER_H
