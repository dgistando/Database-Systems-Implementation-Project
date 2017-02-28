#include <iostream>
#include <string>

#include "Catalog.h"
extern "C" {
#include "QueryParser.h"
}
//stuff
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"

using namespace std;
int printMenu (Catalog cname){
	int answer;
	cout << "MENU: " << endl;
	cout << endl;
	cout << "1) CREATE TABLE" << endl;
	cout << "2) DROP TABLE" << endl;
	cout << "3) VIEW CATALOG" << endl;
	cout << "4) SAVE DATABSE" << endl;
	cin >> answer;
/*	switch(answer){
		case 1:
			cname.CreateTable();
			break;
		case 2:
			cname.DropTable();
			break;
		case 3:
			//multiple functions here?
			break;
		case 4:
			cname.Save();
			break;
	} */
}

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
	string dbFile = "catalog";
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
}

