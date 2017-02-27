#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {

	// create a SCAN operator for each table in the query
	vector<Scan> scans;
	string table;
	unsigned int noDistinct, noTuples;
	while(_tables->next != NULL){
		table = _tables->tableName;
		Schema schema;
		catalog->GetSchema(table, schema);

		DBFile db;

		scans.push_back(Scan(schema, db));

		// push-down selections: create a SELECT operator wherever necessary
		Comparison compare;
		if(ConditionOnSchema(*_predicate, schema)){
			//if so, pass all selection preticates.
			while(_predicate-> rightAnd != NULL){
				ComparisonOp* cOp = _predicate->left;
				
				//NAME op (FLOAT,INTEGER) ex: p.size < 50
				if(cOp->left->code == NAME && cOp->code != EQUALS && cOp->right->code != NAME)
				{
					std::string s(cOp->left->value);
					catalog->GetNoDistinct(table, s, noDistinct);
					noDistinct /= 3;
					catalog->SetNoDistinct(table, s, noDistinct);
				}
				else if(cOp->left->code == NAME && cOp->code == EQUALS && cOp->right->code != NAME)
				{
					std::string s(cOp->left->value);
					catalog->GetNoTuples(table, noTuples);
					catalog->GetNoDistinct(table, s, noDistinct);
					noTuples /= noDistinct;
					catalog->SetNoTuples(table, noTuples);
				}

				_predicate = _predicate->rightAnd;
			}
		}

		_tables = _tables->next;
	}


	
	
	// call the optimizer to compute the join order
	OptimizationTree* root;
	optimizer->Optimize(_tables, _predicate, root);

	// create join operators based on the optimal order computed by the optimizer

	// create the remaining operators based on the query

	// connect everything in the query execution tree and return

	// free the memory occupied by the parse tree since it is not necessary anymore
}
