#include <iostream>
<<<<<<< HEAD
#include <string>
=======
#include <cstdlib>
#include <cstdio>
>>>>>>> c04b4f53c8cd227b4f2bb66b5bf33e560e308393

#include "Catalog.h"
extern "C" {
#include "QueryParser.h"
}
//stuff
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"

using namespace std;


<<<<<<< HEAD
// these data structures hold the result of the parsing
extern struct FuncOperator* finalFunction; // the aggregate function
extern struct TableList* tables; // the list of tables in the query
extern struct AndList* predicate; // the predicate in WHERE
extern struct NameList* groupingAtts; // grouping attributes
extern struct NameList* attsToSelect; // the attributes in SELECT
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query

extern "C" {int yyparse();} //<-- this was uncommented
extern "C" int yylex_destroy();


int main () {
	// this is the catalog
	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);

	// this is the query optimizer
	// it is not invoked directly but rather passed to the query compiler
	QueryOptimizer optimizer(catalog);

	// this is the query compiler
	// it includes the catalog and the query optimizer
	QueryCompiler compiler(catalog, optimizer);


	// the query parser is accessed directly through yyparse
	// this populates the extern data structures
	int parse = -1;
	if (yyparse () == 0) {
		cout << "OK!" << endl;
		parse = 0;
	}
	else {
		cout << "Error: Query is not correct!" << endl;
		parse = -1;
	}

	yylex_destroy();

	if (parse != 0) return -1;

	// at this point we have the parse tree in the ParseTree data structures
	// we are ready to invoke the query compiler with the given query
	// the result is the execution tree built from the parse tree and optimized
	QueryExecutionTree queryTree;
	compiler.Compile(tables, attsToSelect, finalFunction, predicate,
		groupingAtts, distinctAtts, queryTree);

	cout << queryTree << endl;
=======
int main (int argc, char* argv[]) {
	if (argc != 4) {
		cout << "Usage: main [sqlite_file] [no_tables] [no_atts]" << endl;
		return -1;
	}

	string dbFile = argv[1];
	int tNo = atoi(argv[2]);
	int aNo = atoi(argv[3]);

	Catalog catalog(dbFile);
	string tName = "table1";
	string aName = "attr3";
	//cout << catalog << endl; cout.flush();
	unsigned int tuples;
	catalog.GetNoTuples(tName, tuples);
	cout << tName << " tuples = " << tuples << endl;

	string path;
	catalog.GetDataFile(tName, path);
	cout << tName << " path = " << path << endl;
	
	unsigned int distinct;
	catalog.GetNoDistinct(tName, aName, distinct);
	cout << tName << "." << aName << " distinct = " << distinct << endl;
	
	cout<<"Tables"<<endl;
	vector<string> tables;
	catalog.GetTables(tables);
	for (vector<string>::iterator it = tables.begin();it != tables.end(); it++) {
		cout << *it << endl;
	}	
	
	cout<<"attributes for "<<tName<<" "<<endl;
	vector<string> atts;
	catalog.GetAttributes(tName, atts);
	for (vector<string>::iterator it = atts.begin(); it != atts.end(); it++) {
		cout << *it << " ";
	}
	cout << endl;
	
	cout<<"Schema "<<endl;
	Schema schema;
	catalog.GetSchema(tName, schema);
	cout << schema << endl;

	cout<<"Setting tuples: "<<endl;
	tuples = 99;
	catalog.SetNoTuples(tName, tuples);
	
	catalog.GetNoTuples(tName, tuples);
	cout << tName << " tuples = " << tuples << endl;

	
	////////////////////////////////
	/*for (int i = 0; i < tNo; i++) {
		char tN[20];// sprintf(tN, "T_%d", i);
		string tName = tN;

		int taNo = i * aNo;
		vector<string> atts;
		vector<string> types;
		for (int j = 0; j < taNo; j++) {
			char aN[20]; //sprintf(aN, "A_%d_%d", i, j);
			string aName = aN;
			atts.push_back(aN);

			string aType;
			int at = j % 3;
			if (0 == at) aType = "Integer";
			else if (1 == at) aType = "Float";
			else if (2 == at) aType = "String";
			types.push_back(aType);
		}

		bool ret = catalog.CreateTable(tName, atts, types);
		if (true == ret) {
			//cout << "CREATE TABLE " << tName << " OK" << endl;

			for (int j = 0; j < taNo; j++) {
				unsigned int dist = i * 10 + j;
				string aN = atts[j];
				catalog.SetNoDistinct(tName, atts[j], dist);
			}

			unsigned int tuples = i * 1000;
			catalog.SetNoTuples(tName, tuples);

			string path = "/home/user/DATA/" + tName + ".dat";
			catalog.SetDataFile(tName, path);
		}
		else {
			//cout << "CREATE TABLE " << tName << " FAIL" << endl;
		}
	}


	////////////////////////////////
	catalog.Save();
	cout << catalog << endl; cout.flush();


	////////////////////////////////
	vector<string> tables;
	catalog.GetTables(tables);
	for (vector<string>::iterator it = tables.begin();
		 it != tables.end(); it++) {
		cout << *it << endl;
	}
	cout << endl;


	////////////////////////////////
	for (int i = 0; i < 1000; i++) {
		int r = rand() % tNo + 1;
		char tN[20];// sprintf(tN, "T_%d", r);
		string tName = tN;

		unsigned int tuples;
		catalog.GetNoTuples(tName, tuples);
	//	cout << tName << " tuples = " << tuples << endl;

		string path;
		catalog.GetDataFile(tName, path);
	//	cout << tName << " path = " << path << endl;

		vector<string> atts;
		catalog.GetAttributes(tName, atts);
		for (vector<string>::iterator it = atts.begin();
			 it != atts.end(); it++) {
			cout << *it << " ";
		}
		cout << endl;

		Schema schema;
		catalog.GetSchema(tName, schema);
		cout << schema << endl;

		////////////////////////////////
		for (int j = 0; j < 10; j++) {
			int s = rand() % (r * aNo) + 1;
			char aN[20]; //sprintf(aN, "A_%d_%d", r, s);
			string aName = aN;

			unsigned int distinct;
			catalog.GetNoDistinct(tName, aName, distinct);
		//	cout << tName << "." << aName << " distinct = " << distinct << endl;
		}
	}


	////////////////////////////////
	for (int i = 0; i < 5; i++) {
		char tN[20];// sprintf(tN, "T_%d", i);
		string tName = tN;

		bool ret = catalog.DropTable(tName);
		if (true == ret) {
		//	cout << "DROP TABLE " << tName << " OK" << endl;
		}
		else {
		//	cout << "DROP TABLE " << tName << " FAIL" << endl;
		}
	}*/
>>>>>>> c04b4f53c8cd227b4f2bb66b5bf33e560e308393

	return 0;
}
