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
#include <string>
#include <sstream>
#include <vector>
#include <iterator>

using namespace std;


namespace ext{
    template<typename Out>
    void split(const std::string &s, char delim, Out result) {
        std::stringstream ss;
        ss.str(s);
        std::string item;
        while (std::getline(ss, item, delim)) {
            *(result++) = item;
        }
    }
    template < typename T > string to_string( const T& n ){
        ostringstream stm; stm << n; return stm.str() ;
    }

    std::vector<std::string> split(const std::string &s, char delim) {
        std::vector<std::string> elems;
        split(s, delim, std::back_inserter(elems));
        return elems;
    }
}

QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(AttList* attsToCreate, int& queryType, int& numberOfPages,TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate, NameList* _groupingAtts,
	int& _distinctAtts,	QueryExecutionTree& _queryTree) {
    
        // GOOD OLD SELECT QUERIES, MAY THEY DIE IN HELL
        if (queryType == 0){
            // store Scans and Selects for each table to generate Query Execution Tree
            unordered_map<string, RelationalOp*> pushDowns;
            TableList *tblList = _tables;
            while(_tables != NULL) {
                    string tableName = string(_tables->tableName);
                    DBFile dbFile; // nothing to do during phase2
                    string fileLocation = "";
                    string tableNameString = _tables->tableName;
                    catalog->GetDataFile(tableNameString,fileLocation);
                    dbFile.Open(&fileLocation[0]); // just for tableName
                    //int pages = dbFile.Close();
                    //dbFile.Open(&fileLocation[0]);
                    //dbFile.SetPageNums(pages);
                    dbFile.MoveFirst(); // added to move to the first page.


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

                    /** ORIGINAL **/
                    if(cnf.numAnds > 0) {
                            Select* select = new Select(schema, cnf, record, (RelationalOp*) scan);
                            pushDowns[tableName] = (RelationalOp*) select;
                    }
                    /** NEW_START **/
                    /*if(cnf.numAnds > 0) {
                        vector<Attribute> test = schema.GetAtts();
                        string indexfile = test[0].name;
                        string indexheader = test[1].name;
                        ScanIndex* scanIndex = new ScanIndex(schema, cnf, record, indexfile, indexheader, tableName);
                        pushDowns[tableName] = (RelationalOp*) scanIndex;
                    } else {
                        Select* select = new Select(schema, cnf, record, (RelationalOp*) scan);
                        pushDowns[tableName] = (RelationalOp*) select;
                    }*/
                    /** NEW_END **/

                    // move on to the next table
                    _tables = _tables->next;
                    dbFile.Close();
            }

            /** call the optimizer to compute the join order **/
            OptimizationTree root;
            optimizer->Optimize(tblList, _predicate, &root);
            OptimizationTree *rootTree = &root;
            /** create join operators based on the optimal order computed by the optimizer **/
            // get actual Query eXecution Tree by joining them
            RelationalOp* qxTree = buildJoinTree(numberOfPages,rootTree, _predicate, pushDowns, 0);

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

                            //int keepMe[attsToKeep.size()];
                            int * keepMe  = new int[attsToKeep.size()];
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
                            attributeTypes.push_back(compute.GetTypeAsString());
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
                            attributeTypes.push_back(compute.GetTypeAsString());
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
            string outFile = "output.txt";
            Schema outSchema = qxTreeRoot->GetSchema();
            WriteOut* writeOut = new WriteOut(outSchema, outFile, qxTreeRoot);
            qxTreeRoot = (RelationalOp*) writeOut;

            /** connect everything in the query execution tree and return **/
            _queryTree.SetRoot(*qxTreeRoot);

            /** free the memory occupied by the parse tree since it is not necessary anymore **/
            _tables = NULL; _attsToSelect = NULL; _finalFunction = NULL;
            _predicate = NULL; _groupingAtts = NULL; rootTree = NULL;
        } else if (queryType == 1){ // CREATE QUERIES
            vector<string> attributesToMake, typesToMake;
            while(attsToCreate != NULL){
                string attrbt(attsToCreate->attname); attributesToMake.push_back(attrbt);
                string type(attsToCreate->atttype); typesToMake.push_back(type);
                attsToCreate = attsToCreate->next;
            }
            string newTableName(_tables->tableName);
            catalog->CreateTable(newTableName,attributesToMake,typesToMake);
            catalog->Save();
            cout << *catalog << endl;
        } else if (queryType == 2){ // LOAD QUERIES
            string tableToLoadIn (_tables->tableName),
                   textFileToLoadFrom (_attsToSelect->name),
                    tableHeapFile = textFileToLoadFrom + ".dat";
            textFileToLoadFrom += ".tbl";
            DBFile dbfile; dbfile.Create(&tableHeapFile[0],Heap);
            Schema schema; catalog->GetSchema(tableToLoadIn,schema);
            dbfile.Load(schema,&textFileToLoadFrom[0]);
            dbfile.Close();       
        } else if (queryType == 3){ // INDEX QUERIES
            map <int, vector<Record>> mapindex;
            TableList* tabl = _tables;

            string indexName = tabl->tableName;
            tabl = tabl->next;
            string indexTable (tabl->tableName); indexTable += ".dat";
            string indexAtt = _attsToSelect->name;

            //cout<<indexName<<indexTable<<indexAtt<<endl;

            DBFile db, ind, childex;
            db.Open(&indexTable[0]);
            Record record;
            int counter=0;
            int counteri=0, p = 1, r=0;
            //Record rec;

            ind.Create(&indexName[0], Index);

            string childname = "internal_" + indexAtt ;

            vector<string> at; at.push_back(indexName) ; at.push_back(childname);
            vector<string> type; type.push_back("STRING"); type.push_back("STRING");

            catalog->CreateTable(indexAtt, at, type);


            childex.Create(&childname[0], Index);


            catalog->Save();

            at.clear();
            type.clear();
/*
            vector<string> at; at.push_back("key");at.push_back("page");at.push_back("record");
            vector<string> type; type.push_back("INTEGER");type.push_back("INTEGER");type.push_back("INTEGER");
            catalog->CreateTable(indexName, at, type);
            vector<string> lt; lt.push_back("key"); lt.push_back("child");
            vector<string> typ; typ.push_back("INTEGER"); typ.push_back("INTEGER");
            catalog->CreateTable(childname, lt, typ);
*/		

            //Schema sc;
            //catalog->GetSchema(indexName, sc);

            while(db.GetNext(record)) { 

                    counter++;

                    counteri+= record.GetSize();
                    if(counteri > PAGE_SIZE) {p++; r=0; counteri=0;};
                    Schema sch;
                    catalog->GetSchema(indexTable, sch);
                    unsigned int numAtts = sch.GetNumAtts();
                    vector<int> attIndex;
                    attIndex.push_back(sch.Index(indexAtt));	
                    int* attToKeep = &attIndex[0];
                    record.Project (attToKeep,1,numAtts);
                    //cout<<sch<<endl;
                    sch.Project(attIndex);
                    //cout<<sch<<endl;
                    stringstream ss;
                    record.print(ss, sch);
                    //cout<<"record after projection "; record.print(cout, sch); cout<<endl;

                    string s = ss.str();
                    size_t pos = s.find(":");
                    string str = s.substr(pos+2, s.length()-pos-3);
                    //cout<<str;


                    r++;
                    int key = stoi(str, nullptr, 10);
                    //cout<<key;


                    char* recSpace = new char[PAGE_SIZE];
                    int currentPosInRec = sizeof (int) * (2);
                    ((int *) recSpace)[1] = currentPosInRec;
                    *((int *) &(recSpace[currentPosInRec])) = key ;
                    currentPosInRec += sizeof (int);
                    ((int *) recSpace)[0] = currentPosInRec;
                    Record newrec;
                    newrec.CopyBits( recSpace, currentPosInRec );

                    recSpace = new char[PAGE_SIZE];
                    currentPosInRec = sizeof (int) * (2);
                    ((int *) recSpace)[1] = currentPosInRec;
                    *((int *) &(recSpace[currentPosInRec])) = p ;
                    currentPosInRec += sizeof (int);
                    ((int *) recSpace)[0] = currentPosInRec;
                    Record newrec2;
                    newrec2.CopyBits( recSpace, currentPosInRec );

                    recSpace = new char[PAGE_SIZE];
                    currentPosInRec = sizeof (int) * (2);
                    ((int *) recSpace)[1] = currentPosInRec;
                    *((int *) &(recSpace[currentPosInRec])) = r ;
                    currentPosInRec += sizeof (int);
                    ((int *) recSpace)[0] = currentPosInRec;
                    Record newrec3;
                    newrec3.CopyBits( recSpace, currentPosInRec );

                    Record temp;
                    temp.AppendRecords (newrec,newrec2,1,1);
                    newrec.AppendRecords (temp,newrec3,2,1);

                    //newrec.print(cout, sc ); cout<<endl;

                    auto it = mapindex.find(key);
                    if(it == mapindex.end()) {
                            vector<Record> vrec;
                            vrec.push_back(newrec);
                            mapindex[key]=vrec;
                    }
                    else {
                            it->second.push_back(newrec);
                    }			


                    //ind.AppendRecord(newrec);

            }

            counteri=0;
            auto it = mapindex.begin();
            int newkey = it->first;
            int childnum = 1;

    /*	Schema sh;
            catalog->GetSchema(childname, sh);
    */
            char* recSpace = new char[PAGE_SIZE];
            int currentPosInRec = sizeof (int) * (2);
            ((int *) recSpace)[1] = currentPosInRec;
            *((int *) &(recSpace[currentPosInRec])) = newkey ;
            currentPosInRec += sizeof (int);
            ((int *) recSpace)[0] = currentPosInRec;
            Record newrec;
            newrec.CopyBits( recSpace, currentPosInRec );

            recSpace = new char[PAGE_SIZE];
            currentPosInRec = sizeof (int) * (2);
            ((int *) recSpace)[1] = currentPosInRec;
            *((int *) &(recSpace[currentPosInRec])) = childnum ;
            currentPosInRec += sizeof (int);
            ((int *) recSpace)[0] = currentPosInRec;
            Record newrec2;
            newrec2.CopyBits( recSpace, currentPosInRec );

            Record temp;
            temp.AppendRecords (newrec,newrec2,1,1);

            //temp.print(cout, sh);

            childex.AppendRecord(temp);

            for(auto at:mapindex) {
                    for(int i =0; i<at.second.size(); i++)
                            {	
                                    counteri += at.second[i].GetSize();
                                    //at.second[i].print(cout, sc); cout<<endl; 
                                    ind.AppendRecord(at.second[i]);


                                    if(counteri >= PAGE_SIZE) {

                                            newkey = at.first;
                                            childnum++;
                                            counteri=0;

                                            recSpace = new char[PAGE_SIZE];
                                            currentPosInRec = sizeof (int) * (2);
                                            ((int *) recSpace)[1] = currentPosInRec;
                                            *((int *) &(recSpace[currentPosInRec])) = newkey ;
                                            currentPosInRec += sizeof (int);
                                            ((int *) recSpace)[0] = currentPosInRec;
                                            Record newrec;
                                            newrec.CopyBits( recSpace, currentPosInRec );

                                            recSpace = new char[PAGE_SIZE];
                                            currentPosInRec = sizeof (int) * (2);
                                            ((int *) recSpace)[1] = currentPosInRec;
                                            *((int *) &(recSpace[currentPosInRec])) = childnum ;
                                            currentPosInRec += sizeof (int);
                                            ((int *) recSpace)[0] = currentPosInRec;
                                            Record newrec2;
                                            newrec2.CopyBits( recSpace, currentPosInRec );

                                            temp.AppendRecords (newrec,newrec2,1,1);

                                            //temp.print(cout, sh);

                                            childex.AppendRecord(temp);

                                    }
                            }
            }

            childex.Close();
            ind.Close();	
        } else { cout << "ERROR QUERY TYPE" << endl; }
}
// a recursive function to create Join operators (w/ Select & Scan) from optimization result
RelationalOp* QueryCompiler::buildJoinTree(int& numberOfPages,OptimizationTree*& _tree,
	AndList* _predicate, unordered_map<string, RelationalOp*>& _pushDowns, int depth) {
	// at leaf, do push-down (or just return table itself)
	if(_tree->leftChild == NULL && _tree->rightChild == NULL) {
		return _pushDowns.find(_tree->tables[0])->second;
	} else { // recursively do join from left/right RelOps
		Schema lSchema, rSchema, oSchema; CNF cnf;

		RelationalOp* lOp = buildJoinTree(numberOfPages,_tree->leftChild, _predicate, _pushDowns, depth+1);
		RelationalOp* rOp = buildJoinTree(numberOfPages,_tree->rightChild, _predicate, _pushDowns, depth+1);

		lSchema = lOp->GetSchema();
		rSchema = rOp->GetSchema();
		cnf.ExtractCNF(*_predicate, lSchema, rSchema);
		oSchema.Append(lSchema); oSchema.Append(rSchema);
		Join* join = new Join(numberOfPages,lSchema, rSchema, oSchema, cnf, lOp, rOp);

		// set current depth for join operation
		join->depth = depth;
		join->numTuples = _tree->noTuples;

		return (RelationalOp*) join;
	}
}
