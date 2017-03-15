#include <unordered_map>
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
	FuncOperator* _finalFunction, AndList* _predicate, NameList* _groupingAtts,
	int& _distinctAtts,	QueryExecutionTree& _queryTree) {
	// store Scans and Selects for each table to generate Query Execution Tree
	unordered_map<string, RelationalOp*> pushDowns;
	TableList *tblList = _tables;
	while(_tables != NULL) {
		string tableName = string(_tables->tableName);
		DBFile dbFile; // nothing to do during phase2
		dbFile.Create(_tables->tableName, Heap); // just for tableName

		/** create a SCAN operator for each table in the query **/
		Schema schema;
		if(!catalog->GetSchema(tableName, schema)) {
			cerr << "ERROR: Table '" << tableName << "' does not exist" << endl << endl;
			exit(-1);
		}

		Scan* scan = new Scan(schema, dbFile);
		pushDowns[tableName] = (RelationalOp*) scan;

		/** push-down selects: create a SELECT operator wherever necessary **/
		CNF cnf; Record record;
		// CNF::ExtractCNF returns 0 if success, -1 otherwise
		if(cnf.ExtractCNF(*_predicate, schema, record) == -1) {
			// error message is already shown in CNF::ExtractCNF
			exit(-1);
		}

		if(cnf.numAnds > 0) {
			Select* select = new Select(schema, cnf, record, (RelationalOp*) scan);
			pushDowns[tableName] = (RelationalOp*) select;
		}

		// move on to the next table
		_tables = _tables->next;
	}

	/** call the optimizer to compute the join order **/
	OptimizationTree root;
	optimizer->Optimize(tblList, _predicate, &root);
	OptimizationTree *rootTree = &root;
	/** create join operators based on the optimal order computed by the optimizer **/
	// get actual Query eXecution Tree by joining them
	RelationalOp* qxTree = buildJoinTree(rootTree, _predicate, pushDowns, 0);

	/** create the remaining operators based on the query **/
	// qxTreeRoot will be the root of query execution tree
	RelationalOp* qxTreeRoot = (RelationalOp*) qxTree;

	// case 1. SELECT-WHERE-FROM
	// since there is no _groupingAtts, check _finalFunction first.
	// if _finalFunction does not exist, Project and check _distinctAtts for DuplicateRemoval
	// else, append Sum at the root.

	// schemaIn always comes from join tree
	Schema schemaIn = qxTree->GetSchema();

	if(_groupingAtts == NULL) {

		if(_finalFunction == NULL) { // check _finalFunction first
			// a Project operator is appended at the root
			Schema schemaOut = schemaIn;
			int numAttsInput = schemaIn.GetNumAtts();
			int numAttsOutput = 0;
			vector<int> attsToKeep;
			vector<Attribute> atts = schemaIn.GetAtts();
			bool isFound;

			while(_attsToSelect != NULL) {
				string attrName = string(_attsToSelect->name);
				isFound = false;
				for(int i = 0; i < atts.size(); i++) {
					if(atts[i].name == attrName) {
						isFound = true;
						attsToKeep.push_back(i);
						numAttsOutput++;
						break;
					}
				}

				if(!isFound) {
					cerr << "ERROR: Attribute '" << attrName << "' does not exist." << endl << endl;
					exit(-1);
				}

				_attsToSelect = _attsToSelect->next;
			}

			if(schemaOut.Project(attsToKeep) == -1) {
				cerr << "ERROR: Project failed:\n" << schemaOut << endl << endl;
				exit(-1);
			}

			int keepMe[attsToKeep.size()];
			copy(attsToKeep.begin(), attsToKeep.end(), keepMe);

			Project* project = new Project(schemaIn, schemaOut, numAttsInput, numAttsOutput,
				keepMe, qxTree);

			// in case of DISTINCT, a DuplicateRemoval operator is further inserted
			if(_distinctAtts != 0) {
				Schema schemaIn = project->GetSchema();
				DuplicateRemoval* distinct = new DuplicateRemoval(schemaIn, project);
				qxTreeRoot = (RelationalOp*) distinct;
			} else {
				qxTreeRoot = (RelationalOp*) project;
			}
		} else { // a Sum operator is insert at the root
			// the output schema consists of a single attribute 'sum'.
			vector<string> attributes, attributeTypes;
			vector<unsigned int> distincts;

			Function compute; compute.GrowFromParseTree(_finalFunction, schemaIn);

			attributes.push_back("sum");
			attributeTypes.push_back(compute.GetType());
			distincts.push_back(1);
			Schema schemaOut(attributes, attributeTypes, distincts);

			Sum* sum = new Sum(schemaIn, schemaOut, compute, qxTree);
			qxTreeRoot = (RelationalOp*) sum;
		}
	} else { // case 2. SELECT-FROM-WHERE-GROUPBY
		// if query has GROUP BY, a GroupBy operator is appended at the root

		// the output schema contains the aggregate attribute 'sum' on the first position
		vector<string> attributes, attributeTypes;
		vector<unsigned int> distincts;

		Function compute;
		if(_finalFunction != NULL) {
			compute.GrowFromParseTree(_finalFunction, schemaIn);

			attributes.push_back("sum");
			attributeTypes.push_back(compute.GetType());
			distincts.push_back(1);
		}

		// followed by the grouping attributes
		vector<int> attsToGroup; int attsNo = 0;

		while(_groupingAtts != NULL) {
			string attrName = string(_groupingAtts->name);
			int noDistinct = schemaIn.GetDistincts(attrName);
			if(noDistinct == -1) {
				cerr << "ERROR: Attribute '" << attrName << "' does not exist." << endl << endl;
				exit(-1);
			}
			Type attrType = schemaIn.FindType(attrName); string attrTypeStr;
			switch(attrType) {
				case Integer:
					attrTypeStr = "INTEGER";
					break;
				case Float:
					attrTypeStr = "FLOAT";
					break;
				case String:
					attrTypeStr = "STRING";
					break;
				default:
					attrTypeStr = "UNKNOWN";
					break;
			}

			attributes.push_back(attrName);
			attributeTypes.push_back(attrTypeStr);
			distincts.push_back(noDistinct);

			attsToGroup.push_back(schemaIn.Index(attrName));
			attsNo++;

			_groupingAtts = _groupingAtts->next;
		}

		Schema schemaOut(attributes, attributeTypes, distincts);

		int attsOrder[attsToGroup.size()];
		copy(attsToGroup.begin(), attsToGroup.end(), attsOrder);
		OrderMaker groupingAtts(schemaIn, attsOrder, attsNo);

		GroupBy* group = new GroupBy(schemaIn, schemaOut, groupingAtts, compute, qxTree);
		qxTreeRoot = (RelationalOp*) group;
	}

	// in the end, create WriteOut at the root of qxTree
	string outFile = "qxTrees/output.txt";
	Schema outSchema = qxTreeRoot->GetSchema();
	WriteOut* writeOut = new WriteOut(outSchema, outFile, qxTreeRoot);
	qxTreeRoot = (RelationalOp*) writeOut;

	/** connect everything in the query execution tree and return **/
	_queryTree.SetRoot(*qxTreeRoot);

	/** free the memory occupied by the parse tree since it is not necessary anymore **/
	_tables = NULL; _attsToSelect = NULL; _finalFunction = NULL;
	_predicate = NULL; _groupingAtts = NULL; rootTree = NULL;
}

// a recursive function to create Join operators (w/ Select & Scan) from optimization result
RelationalOp* QueryCompiler::buildJoinTree(OptimizationTree*& _tree,
	AndList* _predicate, unordered_map<string, RelationalOp*>& _pushDowns, int depth) {
	// at leaf, do push-down (or just return table itself)
	if(_tree->leftChild == NULL && _tree->rightChild == NULL) {
		return _pushDowns.find(_tree->tables[0])->second;
	} else { // recursively do join from left/right RelOps
		Schema lSchema, rSchema, oSchema; CNF cnf;

		RelationalOp* lOp = buildJoinTree(_tree->leftChild, _predicate, _pushDowns, depth+1);
		RelationalOp* rOp = buildJoinTree(_tree->rightChild, _predicate, _pushDowns, depth+1);

		lSchema = lOp->GetSchema();
		rSchema = rOp->GetSchema();
		cnf.ExtractCNF(*_predicate, lSchema, rSchema);
		oSchema.Append(lSchema); oSchema.Append(rSchema);
		Join* join = new Join(lSchema, rSchema, oSchema, cnf, lOp, rOp);

		// set current depth for join operation
		join->depth = depth;
		join->numTuples = _tree->noTuples;

		return (RelationalOp*) join;
	}
}
