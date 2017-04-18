#ifndef _REL_OP_H
#define _REL_OP_H

#include <iostream>
#include <unordered_set>
#include <unordered_map>
#include <mutex>
#include <pthread.h>
#include <deque>


#include "Schema.h"
#include "Record.h"
#include "DBFile.h"
#include "Function.h"
#include "Comparison.h"
#include "Catalog.h"


using namespace std;


class RelationalOp {
protected:
	// the number of pages that can be used by the operator in execution
	int noPages;
public:
	vector<string> tablesName;
	Catalog * catalog;
	RelationalOp * rootOfOptimizedTree;
	bool noJoin;
	// empty constructor & destructor
	RelationalOp() : noPages(-1) {}
	virtual ~RelationalOp() {}

	// set the number of pages the operator can use
	void SetNoPages(int _noPages) {noPages = _noPages;}

	// every operator has to implement this method
	virtual bool GetNext(Record& _record) = 0;

	/* Virtual function for polymorphic printing using operator<<.
	 * Each operator has to implement its specific version of print.
	 */
    virtual ostream& print(ostream& _os) = 0;

    /* Overload operator<< for printing.
     */
    friend ostream& operator<<(ostream& _os, RelationalOp& _op);

};

class Scan : public RelationalOp {
private:
//	// schema of records in operator
//	Schema schema;

	// physical file where data to be scanned are stored
	DBFile file;

public:
	// schema of records in operator
	Schema schema;
//	DBFile db;
	Scan(Schema& _schema, DBFile& _file);
	virtual ~Scan();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);
};

class Select : public RelationalOp {
private:
//	// schema of records in operator
//	Schema schema;

	// constant values for attributes in predicate
	Record constants;

	// operator generating data
	RelationalOp* producer;

public:

	// selection predicate in conjunctive normal form
	CNF predicate;

	// schema of records in operator
	Schema schema;

	Select(Schema& _schema, CNF& _predicate, Record& _constants,
		RelationalOp* _producer);
	virtual ~Select();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

	virtual RelationalOp* GetProducer();
};

class Project : public RelationalOp {
private:
	// schema of records input to operator
	Schema schemaIn;

	// operator generating data
	RelationalOp* producer;

public:

	vector<int> vect;
	// number of attributes in input records
	int numAttsInput;
	// number of attributes in output records
	int numAttsOutput;
	// index of records from input to keep in output
	// size given by numAttsOutput
	int* keepMe;
	// schema of records output by operator
	Schema schemaOut;

	Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
		int _numAttsOutput, int* _keepMe, RelationalOp* _producer);
	virtual ~Project();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

	virtual RelationalOp* GetProducer();

	void GetSchemaOut(Schema& out);
};

class Join : public RelationalOp {
private:
	// schema of records in left operand
	Schema schemaLeft;
	// schema of records in right operand
	Schema schemaRight;
//	// schema of records output by operator
//	Schema schemaOut;

	// selection predicate in conjunctive normal form
	CNF predicate;

	// operators generating data
	RelationalOp* left;
	RelationalOp* right;
public:

	mutex mtx;

	unordered_map<string, unordered_set<Record*> > tasks[5];

	unordered_map<string, unordered_set<Record*> > tasksRight[5];

	unordered_set<Record*> leftSet;

	unordered_set<Record*> rightSet;

	unordered_set<Record*> retSet;

	bool firstTimeThread = true;
	// schema of records output by operator
	Schema schemaOut;


	void putRecord(Record* _record);

	void popRecord(Record& _record);

//	void * functionWithThreads(void * eachArg);

	Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
		CNF& _predicate, RelationalOp* _left, RelationalOp* _right);

	void SetTablesName(vector<string>& tables);
	vector<string> GetTablesName();

	virtual ~Join();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

	virtual RelationalOp* GetLeft();

	virtual RelationalOp* GetRight();

	virtual void SetLeft(RelationalOp* left);

	virtual void SetRight(RelationalOp* right);
};

class DuplicateRemoval : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// operator generating data
	RelationalOp* producer;

public:
	deque<Record*> dupSet;
	OrderMaker * orderMaker;
	DuplicateRemoval(Schema& _schema, RelationalOp* _producer);
	virtual ~DuplicateRemoval();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

	virtual RelationalOp* GetProducer();
};

class Sum : public RelationalOp {
private:
	// schema of records input to operator
	Schema schemaIn;
	// schema of records output by operator
	Schema schemaOut;

	// function to compute
	Function compute;

	// operator generating data
	RelationalOp* producer;

public:

	bool retOnce = true;

	FuncOperator* parseTree;

	bool isFirst = true;

	deque<Record*> retSet;

	deque<Record*> sumSet;

	int sumInt = 0;

	double sumDouble = 0;

	Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute, FuncOperator* parseTree,
		RelationalOp* _producer);
	virtual ~Sum();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

	virtual RelationalOp* GetProducer();
};

class GroupBy : public RelationalOp {
private:
	// schema of records input to operator
	Schema schemaIn;
	// schema of records output by operator
	Schema schemaOut;

	// grouping attributes
	OrderMaker groupingAtts;
	// function to compute
	Function compute;

	// operator generating data
	RelationalOp* producer;

public:

	bool isFirst = true;

	unordered_map<string, deque<Record*>*> gmap;

	unordered_map<string, deque<Record*>*> newGmap;

	unordered_map<string, int> gmapSumInt;

	unordered_map<string, double> gmapSumDouble;

	FuncOperator* parseTree;

	GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
		Function& _compute,	FuncOperator* _parseTree, RelationalOp* _producer);
	virtual ~GroupBy();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

	virtual RelationalOp* GetProducer();
};

class WriteOut : public RelationalOp {
private:
	// schema of records in operator
	Schema schema;

	// output file where to write the result records
	string outFile;

	// operator generating data
	RelationalOp* producer;

public:
	WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer);
	virtual ~WriteOut();

	virtual bool GetNext(Record& _record);

	virtual ostream& print(ostream& _os);

	virtual RelationalOp* GetProducer();
};


class QueryExecutionTree {
private:
	RelationalOp* root;

public:
	Schema schema;
	vector<int> vect;
	Catalog * catalog;
	QueryExecutionTree() {}
	virtual ~QueryExecutionTree() {}

	void ExecuteQuery();
	void SetRoot(RelationalOp& _root) {root = &_root;}

    friend ostream& operator<<(ostream& _os, QueryExecutionTree& _op);
};

#endif //_REL_OP_H