#include <iostream>
#include <string>

#include "Catalog.h"
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"
extern "C" { // due to "previous declaration with ‘C++’ linkage"
	#include "QueryParser.h"
}

using namespace std;


// these data structures hold the result of the parsing
extern struct FuncOperator* finalFunction; // the aggregate function
extern struct TableList* tables; // the list of tables in the query
extern struct AndList* predicate; // the predicate in WHERE
extern struct NameList* groupingAtts; // grouping attributes
extern struct NameList* attsToSelect; // the attributes in SELECT
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern char* command; // command to execute other functionalities (e.g. exit)

extern "C" int yyparse();
extern "C" int yylex_destroy();


int main () {
	// this is the catalog
	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);
        
        DBFile db;
        string fileName = "/home/farrenkor/Desktop/data";
        vector<string> files;
        vector<string> tables;
        catalog.GetTables(files);
        for (int i = 0; i< files.size(); i++) {
            tables.push_back(files[i]);
            files[i]+= ".tbl";
            files[i].insert(0,"/home/farrenkor/Desktop/data/");
            cout<<files[i]<<endl;
	}
        for (int i = 0; i < files.size(); i++)
	{
            string newFileName = fileName + "/" + tables.at(i) + ".dat";
            char* file = &newFileName[0]; 
            db.Create(file,(FileType) Heap);
            Schema sch;
            /*db.Open(&filename[0]);
            Schema sch;
            Page p;
            Record r;
            int records = 0;
            db.setPage();
            while (db.GetNext(r) != 0) records++;
            cout<<"\ntotal rec "<<records;
            cout<<"\ncounting again\n";

            records = 0;
            db.MoveFirst();
            db.setPage();
            while (db.GetNext(r) != 0) records++;
            cout<<"\ntotal rec "<<records;*/

            catalog.GetSchema(tables[i],sch);
            db.Load(sch, &files[i][0]);

            //cout<<"New CurrentLngth "<<
            db.Close();//<<endl;
	}
        
        
        

//	// this is the query optimizer
//	// it is not invoked directly but rather passed to the query compiler
//	QueryOptimizer optimizer(catalog);
//
//	// this is the query compiler
//	// it includes the catalog and the query optimizer
//	QueryCompiler compiler(catalog, optimizer);
//
//	while(true) {		
//		cout << "sqlite-jarvis> ";
//
//		// the query parser is accessed directly through yyparse
//		// this populates the extern data structures
//		int parse = -1;
//		if (yyparse() == 0) {
//			parse = 0;
//		} else {
//			cout << "Error: Query is not correct!" << endl << endl;
//			parse = -1;
//		}
//
//		yylex_destroy();
//
//		if(parse == 0) {
//			if(command != NULL) {
//				if(strcmp(command, "exit") == 0) {
//					cout << endl << "Bye!" << endl << endl;
//					return 0;
//				} else {
//					cout << endl << "Error: Command not found." << endl << endl;
//				}
//			} else {
//				cout << endl << "OK!" << endl;
//
//				// at this point we have the parse tree in the ParseTree data structures
//				// we are ready to invoke the query compiler with the given query
//				// the result is the execution tree built from the parse tree and optimized
//				QueryExecutionTree queryTree;
//				compiler.Compile(tables, attsToSelect, finalFunction, predicate,
//					groupingAtts, distinctAtts, queryTree);
//
//				cout << queryTree << endl;
//			}
//		}
//		// re-open stdin so that we can start reading from the scratch
//		freopen("/dev/tty", "r", stdin); 
//	}
//	fclose(stdin);
	
	return 0;
}
