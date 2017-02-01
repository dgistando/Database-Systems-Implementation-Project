#include <iostream>
#include <string>

#include "Catalog.h"

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

int main () {
	string db_name = "";
	cout << "Enter the database: ";
	cin >> db_name;
	if(db_name != ""){
		Catalog cname = Catalog(db_name);
		printMenu(cname);
//<<<<<<< HEAD
	//drop a table,
//=======
	}

//>>>>>>> refs/remotes/origin/master

using namespace std;

	return 0;
}

	/*string table = "region", attribute, type;
	vector<string> attributes, types;
	vector<unsigned int> distincts;


extern "C" int yyparse();
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

	return 0;
}
