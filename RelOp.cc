#include <iostream>
#include "RelOp.h"
#include <set>
#include <iomanip>
#include <deque>
#include <sstream>
#include <fstream>
#include <time.h>
#include <unordered_map>
#include <cstring>
#include <cstdio>
#include <cstdlib>

using namespace std;


struct arg_struct {
	unordered_map<string, unordered_set<Record*> > * arg1;
	unordered_set<Record*> * arg2;
	CNF cnf;
	Catalog * catalog;
	Schema schemaLeft;
	Schema schemaRight;
	Join * joinpt;
};





ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}
static bool isEmpty(ifstream& pFile)
{
    return pFile.peek() == std::ifstream::traits_type::eof();
}

static inline bool exists(const string& name) {
    ifstream f(name.c_str());
    return f.good();
}
Scan::Scan(Schema& _schema, DBFile& _file) {
	schema = _schema;
	file = _file;
	//db = _file;
	string prefix = "./";
	prefix = prefix + _file.GetFileName();
	char * path = const_cast<char*>(prefix.c_str());// + _file.GetFileName().c_str());
	//db.Create(path, FileType::Heap);
//	cout<<"_file.GetFileName():"<<_file.GetFileName()<<endl;

//	file.Open(path);

	DBFile db;
	string common = "./";
	string source = common + _file.GetFileName() + ".tbl";
	string binary = common + _file.GetFileName();
//	char * path = const_cast<char*> (_file.GetFileName().c_str());
	ifstream filename(binary);
	if(!exists(binary) || isEmpty(filename)) {
		db.Create(path, FileType::Heap);
	}
	db.Open(path);
	Schema schema;
	string tableNameTmp(_file.GetFileName());
	catalog->GetSchema(tableNameTmp, schema);
//		cout<<"count:"<<schema.GetAtts().size()<<endl;
//		for(int i = 0; i < schema.GetAtts().size(); ++i) {
//			cout<<"line 482 : " << schema.GetAtts()[i].name << endl;
//		}

	char * csvpath = const_cast<char*> (source.c_str());
	db.catalog = catalog;
//	cout<<"line 39 "<<"tableNameTmp "<<tableNameTmp<<"  "<<pathName[0]<<endl;
	db.table = tableNameTmp;
	db.Load(schema, csvpath);
	file = db;
	vector<string> tables = schema.GetTablesName();

//		while(1) {
//			Record record;
//			int ret = db.GetNext(record);
//			if(ret == -1) {
//				break;
//			} else {
//				cout<<"line 494:"<<record.print(cout, schema)<<endl;
//			}
//			cout<<endl;
//		}
	//catalog->GetSchema(tables[0], schema);
	//cout<<"count:"<<schema.GetAtts().size()<<endl;
//	for(int i = 0; i < schema.GetAtts().size(); ++i) {
//		cout<<"line 482 : " << schema.GetAtts()[i].name << endl;
//	}
//	char * csvpath = "/home/xin/postgreSQL-tpch/tpch_2_17_0/dbgen/nation.tbl";
	//db.catalog = catalog;
}

Scan::~Scan() {

}

ostream& Scan::print(ostream& _os) {
	_os << "SCAN:";
	for(auto it = tablesName.begin(); it != tablesName.end(); ++it) {
		_os << *it <<" ";
	}
	return _os;
}

bool Scan::GetNext(Record& _record) {
//	db.Load(schema, csvpath);
	Record recordTmp;
	int ret = file.GetNext(recordTmp);
	if(ret == -1) {
		return false;
	} else {
		_record.Swap(recordTmp);
		return true;
	}
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
	schema.Swap(_schema);
	predicate = _predicate;
	constants.Swap(_constants);
	producer = _producer;
}

Select::~Select() {
	free(producer);
}

ostream& Select::print(ostream& _os) {
	_os << "σ:";
	string tableName;
	for(auto it = tablesName.begin(); it != tablesName.end(); ++it) {
		tableName = *it;
		_os << *it <<" ";
	}

	//string content(constants.GetBits()+8);
//	for(int i = 0; constants.GetBits()[i] != '\0'; ++i) {
//		cout<<constants.GetBits()[i]<<" ,";
//	}

	return _os;// << " filter: "<<constants;
}

RelationalOp * Select::GetProducer() {
	return producer;
}

bool Select::GetNext(Record& _record) {
	Record recordTmpRet;
	Record recordTmp;
	bool ret = producer->GetNext(recordTmpRet);
	if(ret == false) return ret;
	if(predicate.Run(recordTmpRet, constants)) {
		_record.Swap(recordTmpRet);
	} else {
		Record retRecord;
		_record.Swap(retRecord);
	}
	return ret;
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
	free(producer);
}

void Project::GetSchemaOut(Schema& out) {
	schemaOut.Swap(schemaIn);
	schemaOut.Project(this->vect);
	out.Swap(schemaOut);
}

ostream& Project::print(ostream& _os) {
	return _os << "Π";
}

RelationalOp* Project::GetProducer() {
	return producer;
}

bool Project::GetNext(Record& _record) {
	Record recordTmp;
	//todo swap
	bool ret = producer->GetNext(recordTmp);
	if(ret && (recordTmp.GetSize() == 0)) return true;
	if(ret == false) return false;
	//int a[] = {1};
//	cout<<"numAttsOutput:"<<numAttsOutput<<endl;
//	cout<<"numAttsInput:"<<numAttsInput<<endl;
//	for(int i = 0; i < numAttsOutput; ++i) {
//		cout<<"line 134:"<<keepMe[i]<<endl;
//	}
	recordTmp.Project(keepMe, numAttsOutput, numAttsInput);//todo
	_record.Swap(recordTmp);
	return ret;
//	return producer->GetNext(_record);
	//todo project
//	_record.Project()
//	cout<<"well form"<<endl;
}

Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {
	schemaLeft = _schemaLeft;
	schemaRight = _schemaRight;
//	schemaOut = _schemaOut;
	Schema tmpSchema;
	tmpSchema.Swap(schemaLeft);
	tmpSchema.Append(schemaRight);
	_schemaOut.Swap(tmpSchema);
	schemaOut = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right;
	firstTimeThread = true;
}

Join::~Join() {
	free(left);
	free(right);
}

ostream& Join::print(ostream& _os) {
	_os << "⨝: ";
	//set<string> tablesName;

//	for(int i = 0; i < schemaLeft.GetAtts().size(); ++i) {
//		string tableName;
//		catalog->GetTableByAttribute(tableName, schemaLeft.GetAtts()[i].name);
//		tablesName.insert(tableName);
//	}
//	for(int i = 0; i < schemaRight.GetAtts().size(); ++i) {
//		string tableName;
//		catalog->GetTableByAttribute(tableName, schemaRight.GetAtts()[i].name);
//		tablesName.insert(tableName);
//	}

//	cout<<"line 99:"<<schemaOut.tablesName.size();
//	cout<<"line 99 left :"<<schemaLeft.tablesName.size()<<endl;
//	cout<<"line 99 right :"<<schemaRight.tablesName.size()<<endl;
//	for(auto it = tablesName.begin(); it != tablesName.end(); ++it) {
//		_os << *it <<" ";
//	}
	for(auto it = tablesName.begin(); it != tablesName.end(); ++it) {
		_os << *it <<" ";
	}
	return _os;
}

RelationalOp* Join::GetLeft() {
	return left;
}

RelationalOp* Join::GetRight() {
	return right;
}

void Join::SetLeft(RelationalOp * left) {
	this->left = left;
}

void Join::SetRight(RelationalOp * right) {
	this->right = right;
}


void Join::putRecord(Record* _record) {
  // critical section (exclusive access to std::cout signaled by locking mtx):
  mtx.lock();
//  Record* tmp = new Record();
//  tmp->Swap(*_record);
  retSet.insert(_record);
//  if(((int*)_record->GetBits())[12] > 10000)
//	  cout<<"line 292:"<<((int*)_record->GetBits())[12]<<"\n";
//  auto it = retSet.find(_record);
//  cout<<"line 294:"<<(*it)->GetSize()<<"\n";
  mtx.unlock();
}

void Join::popRecord(Record& _record) {
  // critical section (exclusive access to std::cout signaled by locking mtx):
  if(!retSet.empty()) {
	  auto it = retSet.begin();
	  _record.Swap((Record&)(**it));
//	  cout<<"line 304:"<<(_record).GetSize()<<"\n";
	  retSet.erase(retSet.begin());
//	  cout<<"line 306:"<<(_record).GetSize()<<"\n";
  }
//  if(((int*)_record.GetBits())[12] > 10000)
//	  cout<<"line 310:"<<((int*)_record.GetBits())[12]<<"\n";
}
int numOfThread = 7;
//unordered_set<Record*> tasks[7];


void * functionWithThreads(void * eachArg) {
	long long count = 0;
	struct arg_struct *args = (struct arg_struct *)eachArg;
	unordered_map<string, unordered_set<Record*>> * each = (unordered_map<string, unordered_set<Record*>> *) (args->arg1);
	if(!each->empty()) {
		unordered_set<Record*>* right = ((unordered_set<Record*>*) (args->arg2));
			CNF predicateThread = ((CNF) (args->cnf));
			Catalog* catalogThread = ((Catalog*) (args->catalog));
			Schema schemaLeft = (Schema) args->schemaLeft;
			Schema schemaRight = (Schema) args->schemaRight;
			Join * joinpt = (Join*) args->joinpt;
			//cout<<"line 312:"<<leftTableName<<" "<<rightTableName<<"\n";
			//cout<<"right.size:"<<right->size()<<endl;
			//cout<<"left.size:"<<each->size()<<endl;

			int rightcount = 0;
			int eachcount = 0;
				for(auto itR = right->begin(); itR != right->end(); ++itR) {
		//			cout<<"rightcount "<<rightcount++<<endl;
					string valueTmp;
					if(predicateThread.andList[0].op == Equals) {
						if(predicateThread.andList[0].operand1 == Target::Right) {
												string value;
												//enum Type {Integer, Float, String, Name};
												if(predicateThread.andList[0].attType == Type::Integer) {
													char * val1 = (**itR).GetColumn(predicateThread.andList[0].whichAtt1);
													int val1Int = *((int *) val1);
													value = to_string(val1Int);
												} else if(predicateThread.andList[0].attType == Type::Float) {
													char * val1 = (**itR).GetColumn(predicateThread.andList[0].whichAtt1);
													float val1Int = *((double *)  val1);
													value = to_string(val1Int);
												} else {
													char * val1 = (**itR).GetColumn(predicateThread.andList[0].whichAtt1);
													string valueCharTmp(val1);
													value = valueCharTmp;
												}
												valueTmp = value;
											} else if(predicateThread.andList[0].operand2 == Target::Right) {
												string value;
												//enum Type {Integer, Float, String, Name};
												if(predicateThread.andList[0].attType == Type::Integer) {
													char * val1 = (**itR).GetColumn(predicateThread.andList[0].whichAtt2);
													int val1Int = *((int *) val1);
													value = to_string(val1Int);
												} else if(predicateThread.andList[0].attType == Type::Float) {
													char * val1 = (**itR).GetColumn(predicateThread.andList[0].whichAtt2);
													float val1Int = *((double *)  val1);
													value = to_string(val1Int);
												} else {
													char * val1 = (**itR).GetColumn(predicateThread.andList[0].whichAtt2);
													string valueCharTmp(val1);
													value = valueCharTmp;
												}
												valueTmp = value;
											}
						//					if(valueTmp == "116") {
						//						cout<<"line 371"<<endl;
						//					}
						//					valueTmp = "116";
											auto it = each->find(valueTmp);
											if(it != each->end()) {
						//						cout<<"line 376"<<endl;
												for(auto itSet = it->second.begin(); itSet != it->second.end(); ++itSet) {
								//					if(schemaLeft.GetNumAtts() == 12)
								//					cout<<"line 368 "<<((**itSet)).GetSize()<<" "<<((**itR)).GetSize()<<"\n";
									//				cout<<"eachcount "<<eachcount++<<endl;
								//					Record leftRecord;
								//					leftRecord = (Record)(**itSet);
								//					Record rightRecord;
								//					rightRecord = (Record)(**itR);
													++count;
													if(predicateThread.Run(**itSet, **itR)) {
														Record* ret = new Record();
								//						vector<string> attrsLeft;
								//						catalogThread->GetAttributes(leftTableName, attrsLeft);//todo new catalog function
								//						vector<string> attrsRight;
								//						catalogThread->GetAttributes(rightTableName, attrsRight);
										//				for(string one : attrsRight) {
										//					cout<<one<<" ";
										//				}
								//						cout<<"{1"<<endl;
								//						cout<<"line 392 schemaLeft.GetNumAtts:"<<schemaLeft.GetNumAtts()<<"\n";
								//						cout<<"line 393 schemaRight.GetNumAtts()"<<schemaRight.GetNumAtts()<<"\n";
														ret->AppendRecords((Record&)(**itSet), (Record&)(**itR), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());//todo tablesName if empty
														joinpt->putRecord(ret);
								//						if((schemaLeft.GetNumAtts() == 5)) {
								//							ret->AppendRecords((Record&)(**itSet), (Record&)(**itR), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());//todo tablesName if empty
								//							putRecord(ret);
								//						} else {
								//							if((schemaLeft.GetNumAtts() == 0) || (schemaRight.GetNumAtts() == 0))
								//													cout<<"line 3820"<<endl;
								//
								//												cout<<"line 372:"<<((Record&)(**itSet)).GetSize()<<" itSet:"<<((Record&)(**itSet)).GetSize()<<" itR:"<<((Record&)(**itR)).GetSize()<<"\n";
								//												if(((int*)((Record&)(**itSet)).GetBits())[12] > 10000) {
								//													cout<<"greater1 ";
								//													cout<<"line 374:"<<ret->GetSize()<<" ((int*)_record)[12]:" <<((int*)((Record&)(**itSet)).GetBits())[12]<<"\n";
								//												} else {
								//													mtx.lock();
								//													ret->AppendRecords((Record&)(**itSet), (Record&)(**itR), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());//todo tablesName if empty
								//													mtx.unlock();
								//													putRecord(ret);
								//													if(((int*)ret->GetBits())[12] > 10000) {
								//														cout<<"greater2 ";
								//														cout<<"line 374:"<<ret->GetSize()<<" ((int*)_record)[12]:" <<((int*)ret->GetBits())[12]<<"\n";
								//													}
								//												}
								//						}



									//					break;
									//					return;
													}
									//				else if(predicateThread.Run((Record&)(**itR), (Record&)(**it))) {
									//					Record* ret = new Record();
									//					vector<string> attrsLeft;
									//					catalogThread->GetAttributes(leftTableName, attrsLeft);//todo new catalog function
									//					vector<string> attrsRight;
									//					catalogThread->GetAttributes(rightTableName, attrsRight);
									//	//				for(string one : attrsRight) {
									//	//					cout<<one<<" ";
									//	//				}
									//	//				cout<<endl;
								//						cout<<"{2"<<endl;
									//					ret->AppendRecords((Record&)(**itR), (Record&)(**it), attrsRight.size(), attrsLeft.size());//todo tablesName if empty
									//					putRecord(ret);
									//					break;
									////					return;
									//				}
												}
							}
					} else {
						for(auto it = each->begin(); it != each->end(); ++it) {
							for(auto itSet = it->second.begin(); itSet != it->second.end(); ++itSet) {
								if(predicateThread.Run(**itSet, **itR)) {
										Record* ret = new Record();
										ret->AppendRecords((Record&)(**itSet), (Record&)(**itR), schemaLeft.GetNumAtts(), schemaRight.GetNumAtts());//todo tablesName if empty
										joinpt->putRecord(ret);
								}
							}
						}
					}
		//		cout<<"line 277"<<endl;
			}
	}
//		cout<<"line 443 "<<count<<"\n";
}




bool Join::GetNext(Record& _record) {
	clock_t start, finish;
	start = clock();
	while(1) {
		Record* tmp = new Record();
		bool status = this->left->GetNext(*tmp);
		if(status == false) break;
		if(tmp->GetSize() == 0) continue;
		leftSet.insert(tmp);
		if(predicate.andList[0].operand1 == Target::Left) {
			string value;
			//enum Type {Integer, Float, String, Name};
			if(predicate.andList[0].attType == Type::Integer) {
				char * val1 = tmp->GetColumn(predicate.andList[0].whichAtt1);
				int val1Int = *((int *) val1);
				value = to_string(val1Int);
			} else if(predicate.andList[0].attType == Type::Float) {
				char * val1 = tmp->GetColumn(predicate.andList[0].whichAtt1);
				float val1Int = *((double *)  val1);
				value = to_string(val1Int);
			} else {
				char * val1 = tmp->GetColumn(predicate.andList[0].whichAtt1);
				string valueCharTmp(val1);
				value = valueCharTmp;
			}
			if(tasks[tmp->GetSize() % numOfThread].find(value) == tasks[tmp->GetSize() % numOfThread].end()) {
				unordered_set<Record*> tmpSet;
				tmpSet.insert(tmp);
//				cout<<"line 440 "<<tmp->GetSize()<<"\n";
				tasks[tmp->GetSize() % numOfThread].insert(make_pair(value, tmpSet));
			} else {
				tasks[tmp->GetSize() % numOfThread][value].insert(tmp);
//				cout<<"line 443 "<<tmp->GetSize()<<"\n";
			}
		} else if(predicate.andList[0].operand2 == Target::Left) {
			string value;
			if(predicate.andList[0].attType == Type::Integer) {
				char * val1 = tmp->GetColumn(predicate.andList[0].whichAtt2);
				int val1Int = *((int *) val1);
				value = to_string(val1Int);
			} else if(predicate.andList[0].attType == Type::Float) {
				char * val1 = tmp->GetColumn(predicate.andList[0].whichAtt2);
				float val1Int = *((double *)  val1);
				value = to_string(val1Int);
			} else {
				char * val1 = tmp->GetColumn(predicate.andList[0].whichAtt2);
				string valueCharTmp(val1);
				value = valueCharTmp;
			}
			if(tasks[tmp->GetSize() % numOfThread].find(value) == tasks[tmp->GetSize() % numOfThread].end()) {
				unordered_set<Record*> tmpSet;
				tmpSet.insert(tmp);
				tasks[tmp->GetSize() % numOfThread].insert(make_pair(value, tmpSet));
//				cout<<"line 465 "<<tmp->GetSize()<<"\n";
			} else {
				tasks[tmp->GetSize() % numOfThread][value].insert(tmp);
//				cout<<"line 468 "<<tmp->GetSize()<<"\n";
			}
		}
	}
//	rightSet.clear();
	while(1) {
		Record* tmp = new Record();
		bool status = this->right->GetNext(*tmp);
		if(status == false) break;
		if(tmp->GetSize() == 0) continue;
		rightSet.insert(tmp);
//		if((this->right->tablesName[0] == "part") && ((int*)tmp->GetBits())[9] > 1000) {
//			cout<<"line 513\n";
//		}
//		if((this->right->tablesName[0] == "part"))
//			cout<<"line 519"<<rightSet.size()<<endl;
	}
	if(retSet.empty() && firstTimeThread) {
		firstTimeThread = false;
		pthread_t threads[7];
		pthread_attr_t attr;
		void *status;
		pthread_attr_init(&attr);
		pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
		for(int i=0; i < numOfThread; ++i) {
			arg_struct* arg = new arg_struct();
			arg->arg1 = &tasks[i];
			arg->arg2 = &rightSet;
			arg->catalog = catalog;
			arg->cnf = predicate;
			arg->joinpt = this;
			if (Join * joinpt = dynamic_cast<Join *>(this->left)) {
				arg->schemaLeft = joinpt->schemaOut;
			} else if(Select * selectpt = dynamic_cast<Select *>(this->left)) {
				arg->schemaLeft = selectpt->schema;
			} else if(Scan * scanpt = dynamic_cast<Scan *>(this->left)) {
				arg->schemaLeft = scanpt->schema;
			}
			if (Join * joinpt = dynamic_cast<Join *>(this->right)) {
				arg->schemaRight = joinpt->schemaOut;
			} else if(Select * selectpt = dynamic_cast<Select *>(this->right)) {
				arg->schemaRight = selectpt->schema;
			} else if(Scan * scanpt = dynamic_cast<Scan *>(this->right)) {
				arg->schemaRight = scanpt->schema;
			}
//			cout<<"schemaLeft:"<<arg->schemaLeft.GetNumAtts()<<"\n";
//			cout<<"schemaRight:"<<arg->schemaRight.GetNumAtts()<<"\n";
//			cout << "main() : creating thread, " << i << endl;
			int rc = pthread_create(&threads[i], &attr, &functionWithThreads, (void *)(arg));
			if (rc) {
			   cout << "Error:unable to create thread," << rc << endl;
			   fprintf(stderr, "line 238\n");
			   //exit(-1);
			}
		}
		pthread_attr_destroy(&attr);

		for(int i=0; i < numOfThread; i++ ) {
			int rc = pthread_join(threads[i], &status);
			if (rc){
				cout << "Error:unable to join," << rc << endl;
				fprintf(stderr, "line 249\n");
			}
		}
		cout<<"retSet.size():"<<retSet.size()<<"\n";
		if(!retSet.empty()) {
			popRecord(_record);
			return true;
		}
	} else if(retSet.empty()) {
		return false;
	} else {
		popRecord(_record);
		return true;
	}
	return false;
}

DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	schema = _schema;
	producer = _producer;
	orderMaker = new OrderMaker(schema);
}

DuplicateRemoval::~DuplicateRemoval() {
	free(producer);
}

ostream& DuplicateRemoval::print(ostream& _os) {
	_os << "δ";
	for(int i = 0; i < this->schema.GetAtts().size(); ++i) {
		_os <<this->schema.GetAtts()[i].name<<" ";
	}
	return _os;
}
RelationalOp* DuplicateRemoval::GetProducer() {
	return producer;
}

bool DuplicateRemoval::GetNext(Record& _record) {
	while(1) {
		Record* tmp = new Record();
		bool ret = this->producer->GetNext(*tmp);
		if(ret == false) break;
		if(tmp->GetSize() == 0) continue;
		bool isFirst = true;
		for(auto it = dupSet.begin(); it != dupSet.end(); ++it) {
			if(orderMaker->Run((Record&)(**it), (Record&)*tmp) == 0) {
				isFirst = false;
				break;
			}
		}
		if(dupSet.empty() || isFirst == true) {
			dupSet.push_back(tmp);
		}
	}
	if(!dupSet.empty()) {
		Record tmp;
		tmp = *(dupSet.front());
		_record.Swap(tmp);
		dupSet.pop_front();
		return true;
	}
	return false;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute, FuncOperator* _parseTree,
	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	schemaOut = schemaIn;
	compute = _compute;
	producer = _producer;
	parseTree = _parseTree;
	Function copyOfFunction(_compute);
	Type retType = copyOfFunction.RecursivelyBuild(parseTree, schemaIn);
	vector<string> attrsType;
	if (retType == Integer) {
		attrsType.push_back("INTEGER");
	}
	else if (retType == Float) {
		attrsType.push_back("FLOAT");
	}
	vector<string> attrsName;
	attrsName.push_back("sum");
	vector<unsigned int> attrsDist;
	attrsDist.push_back(0);
	Schema sumSchema(attrsName, attrsType, attrsDist);
//	sumSchema.Append(schemaIn);
	_schemaOut.Swap(sumSchema);
}

Sum::~Sum() {
	free(producer);
}

ostream& Sum::print(ostream& _os) {
	return _os << "SUM";
}

RelationalOp* Sum::GetProducer() {
	return producer;
}

bool Sum::GetNext(Record& _record) {
	Type retType;
	while(1) {
		isFirst = false;
		Record * tmp = new Record();
		bool ret = this->producer->GetNext(*tmp);
		if(ret == false) break;
		if(tmp->GetSize() == 0) continue;
		//retSet.push_back(tmp);
		int sumIntArg = 0;
		double sumDoubleArg = 0.0;
		retType = compute.Apply(*tmp, sumIntArg, sumDoubleArg);
		if(retType == Type::Integer) {
			sumInt += sumIntArg;
		} else if(retType == Type::Float) {
			sumDouble += sumDoubleArg;
//			cout<<"line 694:"<<sumDouble<<"\n";
		}
	}
	string tmpNum;
	if(retType == Type::Integer) {
		tmpNum = to_string(sumInt);
	} else if(retType == Type::Float) {
		tmpNum = to_string(sumDouble);
	}
//	cout<<"line 702:"<<tmpNum<<"\n";
	//record
	char* bits;
	char* space = new char[PAGE_SIZE];
	char* recSpace = new char[PAGE_SIZE];
	bits = NULL;
	int currentPosInRec = sizeof (int) * (2);
	for(int i = 0; i < tmpNum.length(); ++i) {
		space[i] = tmpNum[i];
	}
	((int *) recSpace)[1] = currentPosInRec;
	space[tmpNum.length()] = 0;
//	vector<string> attrsType;
	if (retType == Integer) {
		*((int *) &(recSpace[currentPosInRec])) = atoi (space);
		currentPosInRec += sizeof (int);
//		attrsType.push_back("INTEGER");
	}
	else if (retType == Float) {
		*((double *) &(recSpace[currentPosInRec])) = atof (space);
		currentPosInRec += sizeof (double);
//		attrsType.push_back("FLOAT");
	}
	((int *) recSpace)[0] = currentPosInRec;
	bits = new char[currentPosInRec];
	memcpy (bits, recSpace, currentPosInRec);
	delete [] space;
	delete [] recSpace;
	Record sumRecord;
	sumRecord.Consume(bits);
	if(retOnce == true) {
		_record.Swap(sumRecord);
		retOnce = false;
		return true;
	}
	return false;
//
//	vector<string> attrsName;
//	attrsName.push_back("sum");
//	vector<unsigned int> attrsDist;
//	attrsDist.push_back(0);
//	Schema sumSchema(attrsName, attrsType, attrsDist);
//	sumSchema.Append(schemaIn);
//	schemaOut = sumSchema;
}

GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute, FuncOperator* _parseTree, RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
//	_schemaOut.Swap(schemaIn);
//	schemaOut.Swap(schemaIn);
	groupingAtts = _groupingAtts;
	compute = _compute;
	producer = _producer;
	parseTree = _parseTree;
	Function copyOfFunction(_compute);
	Type retType = copyOfFunction.RecursivelyBuild(parseTree, _schemaIn);
	vector<string> attrsType;
	if (retType == Integer) {
		attrsType.push_back("INTEGER");
	}
	else if (retType == Float) {
		attrsType.push_back("FLOAT");
	}
	vector<string> attrsName;
	attrsName.push_back("sum");
	vector<unsigned int> attrsDist;
	attrsDist.push_back(0);
	Schema sumSchema(attrsName, attrsType, attrsDist);
	sumSchema.Append(_schemaIn);
	_schemaOut.Swap(sumSchema);
}

GroupBy::~GroupBy() {
	free(producer);
}

ostream& GroupBy::print(ostream& _os) {
	return _os << "γ:"<<tablesName[0];
}

RelationalOp* GroupBy::GetProducer() {
	return producer;
}

bool GroupBy::GetNext(Record& _record) {
	Type retType;
	if(isFirst == true) {
		isFirst = false;
		while(1) {
				Record * tmp = new Record();
				bool ret = this->producer->GetNext(*tmp);
				if(ret == false) break;
				if(tmp->GetSize() == 0) continue;

				string value;
				//enum Type {Integer, Float, String, Name};
				if(groupingAtts.whichTypes[0] == Type::Integer) {
					char * val1 = tmp->GetColumn(groupingAtts.whichAtts[0]);//only support 1 group by attributes
					int val1Int = *((int *) val1);
					value = to_string(val1Int);
				} else if(groupingAtts.whichTypes[0] == Type::Float) {
					char * val1 = tmp->GetColumn(groupingAtts.whichAtts[0]);
					float val1Int = *((double *)  val1);
					value = to_string(val1Int);
				} else {
					char * val1 = tmp->GetColumn(groupingAtts.whichAtts[0]);
					string valueCharTmp(val1);
					value = valueCharTmp;
				}

				auto itgmap = gmap.find(value);
				if(itgmap != gmap.end()) {
					int sumIntArg = 0;
					double sumDoubleArg = 0.0;
					retType = compute.Apply(*tmp, sumIntArg, sumDoubleArg);
					if(retType == Type::Integer) {
						auto itSumInt = gmapSumInt.find(value);
						itSumInt->second += sumIntArg;
					} else if(retType == Type::Float) {
						auto itSumDouble = gmapSumDouble.find(value);
						itSumDouble->second += sumDoubleArg;
					}
					bool isExist = false;
					for(auto itdeque = itgmap->second->begin(); itdeque != itgmap->second->end(); ++itdeque) {
						if(groupingAtts.Run(**itdeque, *tmp) == 0) {
//							cout<<"line 839\n";
							isExist = true;
							break;
						}
					}
					if(isExist == false) {
						itgmap->second->push_back(tmp);
					}
				} else {
					int sumIntArg = 0;
					double sumDoubleArg = 0.0;
					retType = compute.Apply(*tmp, sumIntArg, sumDoubleArg);
					if(retType == Type::Integer) {
						gmapSumInt.insert(make_pair(value, sumIntArg));
					} else if(retType == Type::Float) {
						gmapSumDouble.insert(make_pair(value, sumDoubleArg));
					}
					deque<Record*> * dequeTmp = new deque<Record*>();
					dequeTmp->push_back(tmp);
					gmap.insert(make_pair(value, dequeTmp));
				}
			}
		for(auto it = gmap.begin(); it != gmap.end(); ++it) {
				string gname = it->first;
				deque<Record*> * dq = it->second;
				if(retType == Type::Integer) {
					string tmpNum;
					tmpNum = to_string(gmapSumInt.find(gname)->second);
					//record
					char* bits;
					char* space = new char[PAGE_SIZE];
					char* recSpace = new char[PAGE_SIZE];
					bits = NULL;
					int currentPosInRec = sizeof (int) * (2);
					for(int i = 0; i < tmpNum.length(); ++i) {
						space[i] = tmpNum[i];
					}
					((int *) recSpace)[1] = currentPosInRec;
					space[tmpNum.length()] = 0;
				//	vector<string> attrsType;
					*((int *) &(recSpace[currentPosInRec])) = atoi(space);
					currentPosInRec += sizeof (int);
			//		attrsType.push_back("INTEGER");
					((int *) recSpace)[0] = currentPosInRec;
					bits = new char[currentPosInRec];
					memcpy (bits, recSpace, currentPosInRec);
					delete [] space;
					delete [] recSpace;
					Record sumRecord;
					sumRecord.Consume(bits);
					auto gmapItems = gmap.find(gname);
					for(auto it = gmapItems->second->begin(); it != gmapItems->second->end(); ++it) {
						Record * tmp = new Record();
						tmp->AppendRecords(sumRecord, **it, 1, this->schemaIn.GetNumAtts());
						((Record*)(*it))->Swap(*tmp);
					}

				} else if(retType == Type::Float) {
					string tmpNum;
					tmpNum = to_string(gmapSumDouble.find(gname)->second);
					//record
					char* bits;
					char* space = new char[PAGE_SIZE];
					char* recSpace = new char[PAGE_SIZE];
					bits = NULL;
					int currentPosInRec = sizeof (int) * (2);
					for(int i = 0; i < tmpNum.length(); ++i) {
						space[i] = tmpNum[i];
					}
					((int *) recSpace)[1] = currentPosInRec;
					space[tmpNum.length()] = 0;
					*((double *) &(recSpace[currentPosInRec])) = atof (space);
					currentPosInRec += sizeof (double);
			//		attrsType.push_back("FLOAT");
					((int *) recSpace)[0] = currentPosInRec;
					bits = new char[currentPosInRec];
					memcpy (bits, recSpace, currentPosInRec);
					delete [] space;
					delete [] recSpace;
					Record sumRecord;
					sumRecord.Consume(bits);
					auto gmapItems = gmap.find(gname);
					for(auto it = gmapItems->second->begin(); it != gmapItems->second->end(); ++it) {
						Record * tmp = new Record();
						tmp->AppendRecords(sumRecord, **it, 1, this->schemaIn.GetNumAtts());
						((Record*)(*it))->Swap(*tmp);
//						auto ans = newGmap.find(gname);
//						if(ans != newGmap.end()) {
//							ans->second->push_back(tmp);
//						} else {
//							deque<Record*> * deqTmp = new deque<Record*>();
//							deqTmp->push_back(tmp);
//							newGmap.insert(make_pair(gname, deqTmp));
//						}
					}
				}
			}
	}

//	cout<<"newGmap:"<<newGmap.size()<<endl;

	//output gmap
	if(!gmap.empty()) {
		int count = 0;
		int size = gmap.size();
		for(auto it1 = gmap.begin(); it1 != gmap.end(); ++it1) {
			++count;
			if((count == size) && (it1->second->empty())) return false;
			if(!it1->second->empty()) {
				Record * output = it1->second->front();
				_record.Swap(*output);
				it1->second->pop_front();
				return true;
			}
		}
	}
}

WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;
}

WriteOut::~WriteOut() {
	free(producer);
}

ostream& WriteOut::print(ostream& _os) {
	return _os << "";
}

RelationalOp* WriteOut::GetProducer() {
	return producer;
}

bool WriteOut::GetNext(Record& _record) {
		bool ret = producer->GetNext(_record);
		if(ret == false) return false;
		if(_record.GetSize() == 0) return true;
		Record recordTmp;
		recordTmp.Swap(_record);
		return true;
//		Schema schema;
//		string tableNameTmp("nation");
//		catalog->GetSchema(tableNameTmp, schema);
//		cout<<recordTmp.print(cout, schema);
}
deque<RelationalOp*> deq;
bool noJoin = false;
int DIST = 7;
void printPostOrder(ostream& _os, RelationalOp* p, int indent=0)
{	if(p == NULL) return;
	if (Join * join = dynamic_cast<Join *>(p)) {
		if(join != NULL) {
			if(join->GetRight()) {
				printPostOrder(_os, join->GetRight(), indent+DIST);
			}
			if (indent) {
				if(join->rootOfOptimizedTree != NULL && join->tablesName == join->rootOfOptimizedTree->tablesName) {

				} else {
					_os << setw(indent) << ' ';
				}
			}
			if (join->GetRight()) {
				if(join->rootOfOptimizedTree != NULL && join->tablesName == join->rootOfOptimizedTree->tablesName) {
					_os<< setw(indent) << ' ' <<" /\n";
				} else {
					_os<<" /\n" << setw(indent) << ' ';
				}
			}
			if(join->rootOfOptimizedTree != NULL && join->tablesName == join->rootOfOptimizedTree->tablesName) {
				while(!deq.empty()) {
					RelationalOp * top = deq.front();
					if (GroupBy * groupBy = dynamic_cast<GroupBy *>(top)) {
							//_os<<"(";
							groupBy->print(_os);
							_os<< "->";
						} else if (DuplicateRemoval * distinct = dynamic_cast<DuplicateRemoval *>(top)) {
							//_os<<"(";
							distinct->print(_os);
							_os<< "->";
						} else if (Sum * sum = dynamic_cast<Sum *>(top)) {
							//_os<<"(";
							sum->print(_os);
							_os<< "->";
						} else if (WriteOut * writeOut = dynamic_cast<WriteOut *>(top)) {
							//_os<<"(";
							writeOut->print(_os);
							//_os<< ")-> ";
						} else if (Project * project = dynamic_cast<Project *>(top)) {
							//_os<<"(";
							project->print(_os);
							_os<< "->";
						}
					deq.pop_front();
				}
			}
			_os<<"(";
			join->print(_os);
			_os<< ")\n ";
			if(join->GetLeft()) {
				_os << setw(indent) << ' ' <<" \\\n";
				printPostOrder(_os, join->GetLeft(), indent+DIST);
			}
		}
		return;
	} else if (Select * select = dynamic_cast<Select *>(p)) {
		if(noJoin) {
			while(!deq.empty()) {
				RelationalOp * top = deq.front();
				if (GroupBy * groupBy = dynamic_cast<GroupBy *>(top)) {
						//_os<<"(";
						groupBy->print(_os);
						_os<< "->";
					} else if (DuplicateRemoval * distinct = dynamic_cast<DuplicateRemoval *>(top)) {
						//_os<<"(";
						distinct->print(_os);
						_os<< "->";
					} else if (Sum * sum = dynamic_cast<Sum *>(top)) {
						//_os<<"(";
						sum->print(_os);
						_os<< "->";
					} else if (WriteOut * writeOut = dynamic_cast<WriteOut *>(top)) {
						//_os<<"(";
						writeOut->print(_os);
						//_os<< ")-> ";
					} else if (Project * project = dynamic_cast<Project *>(top)) {
						//_os<<"(";
						project->print(_os);
						_os<< "->";
					}
				deq.pop_front();
			}
		}
		_os << setw(indent) << ' ';
		_os<<"(";
		select->print(_os);
		_os<< ")\n ";
		if(select->GetProducer() != NULL)
		_os << setw(indent) << ' ' <<" \\\n";
		printPostOrder(_os, select->GetProducer(), indent + DIST);
		return;
	} else if (Scan * scan = dynamic_cast<Scan *>(p)) {
		if(noJoin) {
			while(!deq.empty()) {
				RelationalOp * top = deq.front();
				if (GroupBy * groupBy = dynamic_cast<GroupBy *>(top)) {
						//_os<<"(";
						groupBy->print(_os);
						_os<< "->";
					} else if (DuplicateRemoval * distinct = dynamic_cast<DuplicateRemoval *>(top)) {
						//_os<<"(";
						distinct->print(_os);
						_os<< "->";
					} else if (Sum * sum = dynamic_cast<Sum *>(top)) {
						//_os<<"(";
						sum->print(_os);
						_os<< "->";
					} else if (WriteOut * writeOut = dynamic_cast<WriteOut *>(top)) {
						//_os<<"(";
						writeOut->print(_os);
						//_os<< ")-> ";
					} else if (Project * project = dynamic_cast<Project *>(top)) {
						//_os<<"(";
						project->print(_os);
						_os<< "->";
					}
				deq.pop_front();
			}
		}
		//_os << setw(indent) << ' ' <<" \\\n";
		_os << setw(indent) << ' ';
		_os<<"(";
		scan->print(_os);
		_os<< ")\n ";
		return;
	} else if (GroupBy * groupBy = dynamic_cast<GroupBy *>(p)) {
		deq.push_back(groupBy);
		printPostOrder(_os, groupBy->GetProducer(), indent + DIST);
	} else if (DuplicateRemoval * distinct = dynamic_cast<DuplicateRemoval *>(p)) {
		deq.push_back(distinct);
		printPostOrder(_os, distinct->GetProducer(), indent + DIST);
	} else if (Sum * sum = dynamic_cast<Sum *>(p)) {
		deq.push_back(sum);
		printPostOrder(_os, sum->GetProducer(), indent + DIST);
	} else if (WriteOut * writeOut = dynamic_cast<WriteOut *>(p)) {
		deq.push_back(writeOut);
		if(writeOut->rootOfOptimizedTree == NULL) noJoin = true;
		printPostOrder(_os, writeOut->GetProducer(), indent + DIST);
	} else if (Project * project = dynamic_cast<Project *>(p)) {
		deq.push_back(project);
		printPostOrder(_os, project->GetProducer(), indent + DIST);
	}
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	_os<<"##############print Query Execution Tree####################"<<endl;
	printPostOrder(_os, _op.root);
	return _os << "QUERY EXECUTION TREE";
}

void split(string& s, char delim,vector<string>& v) {
    auto i = 0;
    auto pos = s.find(delim);
    while (pos != string::npos) {
      v.push_back(s.substr(i, pos-i));
      i = ++pos;
      pos = s.find(delim, pos);

      if (pos == string::npos)
         v.push_back(s.substr(i, s.length()));
    }
}

void QueryExecutionTree::ExecuteQuery() {
	if(WriteOut * writeOut = dynamic_cast<WriteOut *>(root)) {
		while(1) {
//			cout<<"line 524"<<"\n";
			Record recordTmp;
			bool ret = writeOut->GetProducer()->GetNext(recordTmp);
			if(ret == false) return;
			if(recordTmp.GetSize() == 0) continue;//filter may return 0.
//			cout<<"recordTmp.GetSize():"<<recordTmp.GetSize()<<endl;
//			Schema schema1;
//			schema.Swap(this->schema);
//			string tableName("customer");
//			catalog->GetSchema(tableName, schema1);
			//if record has been changed, how to change the schema.
			//vect.push_back(0);
//			vector<int> v;
//			v.push_back(1);
//			v.push_back(2);
//			v.push_back(5);
//			this->schema.Project(vect);
//			schema1.Project(v);
			stringstream ss;
			recordTmp.print(ss, schema);
			cout<<ss.str()<<"\n";
//			vector<string> content;
//			string arg = ss.str();
//			split(arg, '{', content);
//			for(int i = 0; i < content.size(); ++i) {
//				string tmp = content[i];
//				cout<<tmp<<"\n";
//			}
		}
	}
}