#include <iostream>
#include <sstream>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
	schema = _schema;
	file = _file;
        file.Open(&file.fileName[0]);
        file.MoveFirst();
}

Scan::~Scan() {
}

ostream& Scan::print(ostream& _os) {
	//return _os << file.GetTableName();
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
    if (producer->GetNext(record)) {
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

bool Join::GetNext(Record& record)
{
	Schema left_copy = schemaLeft;
	Record temp;
    while (right->GetNext(temp) == true){            //run only once
        List.Insert(temp);
    }
    while (left->GetNext(temp)==true){
        if (List.Find(temp,join_atts)==true){
            create Record(record, temp, found_rec); //I actually don't know what I wrote here, my b fam
            return true;
        }
    }
}
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

bool DuplicateRemoval::GetNext(Record& record){

	//HERE
    while (1)
	{
		if (! producer->GetNext(record)) return false;
		stringstream s;
		record.print(s, schema);
		auto it = set.find(s.str());
		if(it == set.end()) 
		{
			set[s.str()] = record;
			return true;
		}
	}
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
	recSent = 0;
}

Sum::~Sum() {
}

bool Sum::GetNext(Record& record){
 
	if (recSent) return false;
	int iSum = 0;
	double dSum = 0;
        int counter = 0;
	while(producer->GetNext(record))
	{
		int iResult = 0;
		double dResult = 0;
		Type t = compute.Apply(record, iResult, dResult);
		//cout <<iResult<<endl;
		//cout <<dResult<<endl;
                
		if (t == Integer) iSum+= iResult;
		if (t == Float) dSum+= dResult;
                counter++;
	}
	double val = dSum + (double)iSum;
	char* recSpace = new char[PAGE_SIZE];
	int currentPosInRec = sizeof (int) * (2);
	((int *) recSpace)[1] = currentPosInRec;
	*((double *) &(recSpace[currentPosInRec])) = val;
	currentPosInRec += sizeof (double);
	((int *) recSpace)[0] = currentPosInRec;
	Record sumRec;
	sumRec.CopyBits( recSpace, currentPosInRec );
	delete [] recSpace;
	record = sumRec;
	recSent = 1;
	return true;


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
        phase = 0;
        
        
}

GroupBy::~GroupBy() {
}


bool GroupBy::GetNext(Record& record)
{
	vector<int> attsToKeep, attsToKeep1;
	for (int i = 1; i < schemaOut.GetNumAtts(); i++)
		attsToKeep.push_back(i);

	copy = schemaOut;
	copy.Project(attsToKeep);

	attsToKeep1.push_back(0);
	sum = schemaOut;
	sum.Project(attsToKeep1);

	if (phase == 0)
	{
		while (producer->GetNext(record))	
		{	
			stringstream s;
			int iResult = 0;
			double dResult = 0;
			compute.Apply(record, iResult, dResult);
			double val = dResult + (double)iResult;
			
			record.Project(&groupingAtts.whichAtts[0], groupingAtts.numAtts , copy.GetNumAtts());
			record.print(s, copy);
			auto it = set.find(s.str());

			if(it != set.end())	set[s.str()]+= val;
			else
			{
				set[s.str()] = val;
				recMap[s.str()] = record;
			}
		
		}
		phase = 1;
	}

	if (phase == 1)
	{
		if (set.empty()) return false;

		Record temp = recMap.begin()->second;
		string strr = set.begin()->first;

		char* recSpace = new char[PAGE_SIZE];
		int currentPosInRec = sizeof (int) * (2);
		((int *) recSpace)[1] = currentPosInRec;
		*((double *) &(recSpace[currentPosInRec])) = set.begin()->second;
		currentPosInRec += sizeof (double);
		((int *) recSpace)[0] = currentPosInRec;
		Record sumRec;
		sumRec.CopyBits( recSpace, currentPosInRec );
		delete [] recSpace;
		
		Record newRec;
		newRec.AppendRecords(sumRec, temp, 1, schemaOut.GetNumAtts()-1);
		recMap.erase(strr);
		set.erase(strr);
		record = newRec;
		return true;
	}
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
