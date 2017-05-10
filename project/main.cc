#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <iterator>

#include <fstream>

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
//PROJECT 6
extern struct AttList* attsToCreate; //the type connected via Parser.y
extern int queryType; //the type connected via Parser.y

template<typename Out>
void split(const std::string &s, char delim, Out result) {
    std::stringstream ss;
    ss.str(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        *(result++) = item;
    }
}




std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    split(s, delim, std::back_inserter(elems));
    return elems;
}

int main () {
        //NUMBER OF PAGES
        int numPages = 100;
	// this is the catalog
	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);
        
        //cout<<"Catalog:: "<<catalog<<endl;
        
        string fileName1 = "/home/farrenkor/Desktop/data";
        vector<string> files1;
        vector<string> tables1;
        catalog.GetTables(files1);
        for (int i = 0; i< files1.size(); i++) {
            tables1.push_back(files1[i]);
            files1[i]+= ".tbl";
            files1[i].insert(0,"/home/farrenkor/Desktop/data/");
            cout<<files1[i]<<endl;
	}
        
        for (int i = 0; i < files1.size(); i++)
	{
            break;
            string path = "Heaps//" + tables1.at(i) + ".txt";
            ofstream myfile;
            myfile.open(path);
            
            
            //break;
            DBFile db1;
            //i = 0;
            string newFileName = fileName1 + "/" + tables1.at(i) + ".dat";
            char* file = &newFileName[0]; 
            Schema sch;
            catalog.GetSchema(tables1[i],sch);
            
            db1.Create(file,(FileType) Heap);
            
            //db1.Open(file);
            
            db1.Load(sch, &files1[i][0]);
            int pages = db1.Close();
            cout << "\n pages after closing: " << pages << endl;
            
            DBFile db2;
            db2.Open(file);
            Record r;
            int records = 0;
            //db2.MoveFirst();
            while (db2.GetNext(r) != 0) {
                r.print(myfile,sch);
                myfile << endl;
                cout << endl;
                records++;
            }
            r.print(cout,sch);
            cout<<"\n " << tables1[i] << " total stored: "<< records << endl;
            
            db1.Close();//<<endl;
            //break;
            myfile.close();
	}
        
        

	// this is the query optimizer
	// it is not invoked directly but rather passed to the query compiler
	QueryOptimizer optimizer(catalog);

	// this is the query compiler
	// it includes the catalog and the query optimizer
	QueryCompiler compiler(catalog, optimizer);
        
        
        while(true){
            int n = 0;
            cout << "1. Print Catalog " << endl;
            cout << "2. Run Query " << endl;
            cout << "3. Exit " << endl;
            cin >> n;

            switch(n) {
                case 1:{
                    cout << catalog << endl;
                    break;
                }
                case 2:{
                    //while(true) {		
                        cout << "\n<sqlite> ";

                        // the query parser is accessed directly through yyparse
                        // this populates the extern data structures
                        int parse = -1;
                        if (yyparse() == 0) {
                                parse = 0;
                        } else {
                                cout << "Error: Query is not correct!" << endl << endl;
                                parse = -1;
                        }

                        yylex_destroy();

                        if(parse == 0) {
                                if(command != NULL) {
                                        if(strcmp(command, "exit") == 0) {
                                                cout << endl << "Bye!" << endl << endl;
                                                return 0;
                                        } else {
                                                cout << endl << "Error: Command not found." << endl << endl;
                                        }
                                } else {
                                        cout << endl << "OK!" << endl;

                                        // at this point we have the parse tree in the ParseTree data structures
                                        // we are ready to invoke the query compiler with the given query
                                        // the result is the execution tree built from the parse tree and optimized
                                        QueryExecutionTree queryTree;
                                        compiler.Compile(attsToCreate,queryType,numPages,tables, attsToSelect, finalFunction, predicate, groupingAtts, distinctAtts, queryTree);
                                        if(queryType == 3 || queryType == 2 || queryType == 1){
                                            continue;
                                        }
                                        cout << queryTree << endl;
                                        queryTree.ExecuteQuery();
                                }
                        }
                        // re-open stdin so that we can start reading from the scratch
                        freopen("/dev/tty", "r", stdin);
                //}
                //fclose(stdin);
                    break;
                }
                case 3:{
                    return 0;
                }
            }
        }
	return 0;
}
