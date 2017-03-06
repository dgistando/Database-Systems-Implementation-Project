#include <iostream>
#include "RelOp.h"
#include "Schema.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
		schema = schema;
		file = _file;

}

Scan::~Scan() {
    cout << "Destroy Select" << endl;
}

ostream& Scan::print(ostream& _os) {
	_os << schema;
	return _os << "SCAN";
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
		schema = _schema;
		predicate = _predicate;
		constants = _constants;
		producer = _producer;

}

Select::~Select() {
	delete producer;
}

ostream& Select::print(ostream& _os) {
	return _os << "SELECT";
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
		
		schemaIn = _schemaIn;
		schemaOut = _schemaOut;
		numAttsInput = _numAttsInput;
		numAttsOutput = _numAttsOutput;
		producer = _producer;

}

Project::~Project() {
	delete producer;
}

ostream& Project::print(ostream& _os) {
	return _os << "PROJECT";
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {
		
		schemaLeft = _schemaLeft;
		schemaRight = _schemaRight;
		schemaOut = _schemaOut;
		predicate = _predicate;
		left = _left;
		right = _right;

}

Join::~Join() {
	delete left;
	delete right;
}

ostream& Join::print(ostream& _os) {
	return _os << "JOIN";
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	schema = _schema;
	producer = _producer;

}

DuplicateRemoval::~DuplicateRemoval() {
	delete producer;
}

ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT";
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {
		
		schemaIn = _schemaIn;
		schemaOut = _schemaOut;
		compute = _compute;
		producer = _producer;

}

Sum::~Sum() {
delete producer;

}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM";
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute, RelationalOp* _producer) {
		
		schemaIn = _schemaIn;
		schemaOut = _schemaOut;
		groupingAtts = _groupingAtts;
		compute = _compute;
		producer = _producer;
}

GroupBy::~GroupBy() {
	delete producer;
}

ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY";
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;
}

WriteOut::~WriteOut() {
	delete producer;
}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT";
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE";
}
