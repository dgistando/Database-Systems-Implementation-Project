#include <iostream>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
	schema = _schema;
	file = _file;
}

Scan::~Scan() {
}

ostream& Scan::print(ostream& _os) {
	return _os << file.GetTableName();
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;
}

Select::~Select() {
}

bool Select::GetNext(Record& rec) {
    if (!producer->GetNext(rec)) return false;
    while (!predicate.Run(rec,constants)) {
        if (!producer->GetNext(rec)) return false;
    } return true;
}


ostream& Select::print(ostream& _os) {
	_os << "σ [";
	for(int i = 0; i < predicate.numAnds; i++) {
		if(i > 0) {
			_os << " AND ";
		}

		Comparison comp = predicate.andList[i];
		vector<Attribute> atts = schema.GetAtts();
		if(comp.operand1 != Literal) {
			_os << atts[comp.whichAtt1].name;
		} else { // see Record::print for more info
			int pointer = ((int *) constants.GetBits())[comp.whichAtt1 + 1];
			if (atts[comp.whichAtt1].type == Integer) {
				int *myInt = (int *) &(constants.GetBits()[pointer]);
				_os << *myInt;
			} else if (atts[comp.whichAtt1].type == Float) {
				double *myDouble = (double *) &(constants.GetBits()[pointer]);
				_os << *myDouble;
			} else if (atts[comp.whichAtt1].type == String) {
				char *myString = (char *) &(constants.GetBits()[pointer]);
				_os << myString;
			}
		}

		if (comp.op == LessThan) {
			_os << " < ";
		} else if (comp.op == GreaterThan) {
			_os << " > ";
		} else if (comp.op == Equals) {
			_os << " = ";
		} else {
			_os << " ? ";
		}

		if(comp.operand2 != Literal) {
			_os << atts[comp.whichAtt2].name;
		} else { // see Record::print for more info
			int pointer = ((int *) constants.GetBits())[comp.whichAtt2 + 1];
			if (atts[comp.whichAtt1].type == Integer) {
				int *myInt = (int *) &(constants.GetBits()[pointer]);
				_os << *myInt;
			} else if (atts[comp.whichAtt1].type == Float) {
				double *myDouble = (double *) &(constants.GetBits()[pointer]);
				_os << *myDouble;
			} else if (atts[comp.whichAtt1].type == String) {
				char *myString = (char *) &(constants.GetBits()[pointer]);
				_os << "\'" << myString << "\'";
			}
		}
	}
	_os << "] ── " << *producer;
	return _os;
	// return _os << "σ [...] ── " << *producer; // print without predicates
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	numAttsInput = _numAttsInput;
	numAttsOutput = _numAttsOutput;
	keepMe = _keepMe;
	producer = _producer;
}

Project::~Project() {
}
bool Project::GetNext(Record& record) {
    if (producer->GetNext(record))
    {
            record.Project(keepMe, numAttsOutput, numAttsInput);		
            return true;
    }
    return false;
}

ostream& Project::print(ostream& _os) {
	_os << "π [";
	vector<Attribute> atts = schemaOut.GetAtts();
	for(auto it = atts.begin(); it != atts.end(); it++) {
		if(it != atts.begin())
			_os << ", ";
		_os << it->name;
	}
	_os << "]\n\t │\n\t" << *producer;
	return _os;
	// return _os << "π [...]\n\t │\n\t" << *producer; // print without predicates
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
}

ostream& Join::print(ostream& _os) {
	_os << "⋈ [";
	for(int i = 0; i < predicate.numAnds; i++) {
		if(i > 0) {
			_os << " AND ";
		}

		Comparison comp = predicate.andList[i];

		if(comp.operand1 == Left) {
			_os << schemaLeft.GetAtts()[comp.whichAtt1].name;
		} else if(comp.operand1 == Right) {
			_os << schemaRight.GetAtts()[comp.whichAtt1].name;
		}

		if (comp.op == LessThan) {
			_os << " < ";
		} else if (comp.op == GreaterThan) {
			_os << " > ";
		} else if (comp.op == Equals) {
			_os << " = ";
		} else {
			_os << " ? ";
		}

		if(comp.operand2 == Left) {
			_os << schemaLeft.GetAtts()[comp.whichAtt2].name;
		} else if(comp.operand2 == Right) {
			_os << schemaRight.GetAtts()[comp.whichAtt2].name;
		}
	}
	_os << "]";
	_os << ", Number of Tuples = "<<numTuples;
	// _os << "⋈ [...]"; // print without predicates

	_os << "\n";
	for(int i = 0; i < depth+1; i++)
		_os << "\t";
	_os << " ├──── " << *right;

	_os << "\n";
	for(int i = 0; i < depth+1; i++)
		_os << "\t";
	_os << " └──── " << *left;

	return _os;
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	schema = _schema;
	producer = _producer;
}

DuplicateRemoval::~DuplicateRemoval() {
}

ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "δ \n\t │\n\t" << *producer;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	compute = _compute;
	producer = _producer;
}

Sum::~Sum() {
}

ostream& Sum::print(ostream& _os) {
	_os << "SUM(";
	// do something
	_os << ")\n\t │\n\t" << *producer;;
	return _os;

	// return _os << "SUM(...)\n\t │\n\t" << *producer; // print without predicates
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	groupingAtts = _groupingAtts;
	compute = _compute;
	producer = _producer;
}

GroupBy::~GroupBy() {
}

ostream& GroupBy::print(ostream& _os) {
	_os << "γ [";
	vector<Attribute> atts = schemaOut.GetAtts();
	for(auto it = atts.begin(); it != atts.end(); it++) {
		if(it != atts.begin())
			_os << ", ";

		string attrName = it->name;
		if(attrName == "sum") {
			_os << "SUM(";

			_os << ")";
		} else {
			_os << attrName;
		}
	}
	_os << "]\n\t │\n\t" << *producer;
	return _os;
	// return _os << "γ [...]\n\t │\n\t" << *producer; // print without predicates
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;
}

WriteOut::~WriteOut() {
}

bool WriteOut::GetNext(Record& record) {
    bool writeout = producer->GetNext(record);
    if (!writeout) { return false; }
    return writeout;

}

ostream& WriteOut::print(ostream& _os) {
	return _os << endl << "\t" << *producer << endl << endl;
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE {" << endl << *_op.root << "}" << endl;
}
