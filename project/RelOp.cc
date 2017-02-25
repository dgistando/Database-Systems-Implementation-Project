#include <iostream>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
	//assign values for schema and the database file
}

Scan::~Scan() {

}

ostream& Scan::print(ostream& _os) {
	return _os << "SCAN";
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
	//initialize all the parameters;
}

Select::~Select() {

}

ostream& Select::print(ostream& _os) {
	return _os << "SELECT";
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
	//same
}

Project::~Project() {
	//destroy pointers created in constructor (keepMe)
}

ostream& Project::print(ostream& _os) {
	return _os << "PROJECT";
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {
	//another initializer
}

Join::~Join() {
	//destroy RelationOp left and right
}

ostream& Join::print(ostream& _os) {
	return _os << "JOIN";
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	//another constructor
}

DuplicateRemoval::~DuplicateRemoval() {

}

ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT";
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {

}

Sum::~Sum() {

}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM";
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {

}

GroupBy::~GroupBy() {

}

ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY";
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {

}

WriteOut::~WriteOut() {

}

ostream& WriteOut::print(ostream& _os) {
	return _os << "OUTPUT";
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE";
}
