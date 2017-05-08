#include <iostream>
#include <list>
#include "RelOp.h"

using namespace std;

namespace ext{
    template <typename T>
    void remove(vector<T>& vec, size_t pos)
    {
        typename vector<T>::iterator it = vec.begin();
        advance(it, pos);
        vec.erase(it);
    }
}

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

ScanIndex::ScanIndex(Schema& _schema, CNF& _predicate, Record& _constants, 
		     string& _indexfile, string& _indexheader, string& _table) {

	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	leaf = _indexfile;
	internal = _indexheader;
	table = _table;
	once =0;
	veccount=0;
	//Record fals;
	//finalrec.push_back(fals);
}

bool ScanIndex::GetNext(Record& rec) {

	//vector<Record>finalrec;
	if(once==0) { //cout<<"here"<<endl;
		DBFile mainfile;
		mainfile.Open(&table[0]);

		DBFile f_leaf, f_internal;
		f_leaf.Open(&leaf[0]);
		f_internal.Open(&internal[0]);

		vector<string> at; at.push_back("value") ;
		vector<string> type; type.push_back("INTEGER");
		vector<unsigned int> dis; dis.push_back(0);
		Schema sche(at, type, dis);

		stringstream ss;	

		constants.print(ss, sche); //cout<<endl;
		string s = ss.str();
		size_t pos = s.find(":");
		string str = s.substr(pos+2, s.length()-pos-3);
		//cout<<endl<<str<<endl;

		Record internalrecord;
		f_internal.MoveFirst();
		int counter=1;
		int childcount=0;

		while(f_internal.GetNext(internalrecord)){
		
		
			at.clear();
			type.clear();
			dis.clear();

			at.push_back("key") ; at.push_back("child");
			type.push_back("INTEGER"); type.push_back("INTEGER");
			dis.push_back(0); dis.push_back(0);
			Schema sc(at, type, dis);

			stringstream ssindex;

			internalrecord.print(ssindex, sc); cout<<endl;
			s = ssindex.str();
			//cout<<s<<endl;
			pos = s.find(":");
			size_t pos1 = s.find(",");
			string internalkey = s.substr(pos+1, pos1-pos-1);
			//cout<<endl<<internalkey<<endl;
		
			if(stoi(str)<stoi(internalkey)) { childcount = counter-1; break;}
			else childcount = counter;
			counter++;
		}
		//cout<<"child "<<childcount<<" counter "<<counter;
	
		Page p;
		Record r;
		f_leaf.GetPageNo(childcount-1, p);
		int count=0, count1=0;
		vector<Record> indexrecvec;
	
		at.clear();
		type.clear();
		dis.clear();	
		at.push_back("key");at.push_back("page");at.push_back("record");
		type.push_back("INTEGER");type.push_back("INTEGER");type.push_back("INTEGER");
		dis.push_back(0); dis.push_back(0); dis.push_back(0);

		Schema leafschema(at, type, dis);


		while(p.GetFirst(r) != 0) {

			count++;
	
			stringstream leafrec;
			r.print(leafrec, leafschema);

			string a, keynum, c, pagenum, e, recnum;
			int kn=0, pn=0, rn=0;

			leafrec>>a>>keynum>>c>>pagenum>>e>>recnum;

			keynum.pop_back();
			pagenum.pop_back();
			recnum.pop_back();
			kn=stoi(keynum, nullptr,10); pn=stoi(pagenum, nullptr, 10); rn=stoi(recnum, nullptr, 10);

			int valTocompare = stoi(str, nullptr, 10);

			if(valTocompare == kn) 
			{

				count1++;

				cout<<"page"<<pn<<" ";

				cout<<"record"<<rn<<" "<<"count "<<count1 <<endl;
	
				Record mainrec;
			
				mainfile.GetSpecificRecord(pn-1, rn, mainrec);
				//cout<<"here";
		
				finalrec.push_back(mainrec);
				//cout<<"here";
				//mainrec.print(cout, schema); cout<<endl;

			
			}

			//cout<<kn<<endl<<pn<<endl<<rn<<endl;
			 
		}
		
		
		//cout<<"vector size "<<finalrec.size();
	
		once=1;
		cout<<endl<<"number total records "<<count<<" number applicable records "<<count1<<endl;
		//cout<<"HERE in Scan Index"<<endl;
		//return false;
	
	}
		
	if(veccount<finalrec.size()) {
		rec = finalrec[veccount];
		veccount++;
		return true;
	}
	else return false;
			

}


ScanIndex::~ScanIndex() {
}

ostream& ScanIndex::print(ostream& _os) {
	return _os << "SCAN INDEX";
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
    while(producer->GetNext(rec)){
        if(predicate.Run(rec,constants)){ return true; }
    } return false;
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
    } return false;
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
struct Tuple{
    Record rec;
    int heapIndex;
};
struct RecordComp{
    bool operator() (Record& left, Record& right){return left < right;}
};
struct TupleComp{
    bool operator() (Tuple& left, Tuple& right){return left.rec < right.rec;}
};


int Join::GenerateFinalHeap(Table table){
    
    string testPath = "Heaps//text_" + to_string(rand()) + ".txt";
    ofstream myfile (testPath);
    myfile.is_open() ; 
    
    int counter = 0;
    
    vector<Tuple> tupleList;
    if(table == TableLeft){
        //CREATE FINAL HEAP
        DBFile finalHeap;
        string path = "Heaps//lhf_" + to_string(rand()) + ".dat";
        finalHeap.Create(&path[0], Sorted);
        
        //POPULATE REPLENISHER
        for(int i = 0; i < leftTableHeaps.size(); i++){
            if(!leftTableHeaps[i].isOpen) { leftTableHeaps[i].Open(); cout << "Opening Heap: " << leftTableHeaps[i].fileName << endl; }
            leftTableHeaps[i].MoveFirst();
            Record temp;
            if(leftTableHeaps[i].GetNext(temp)){
                temp.SetOrderMaker(leftOrder);
                Tuple tuple; tuple.heapIndex = i; tuple.rec = temp;
                tupleList.push_back(tuple);
            } else { cout << "Error" << endl; return -1; }
        }
        
        for(int i = 0; i < tupleList.size(); i++){ cout << "BEFORE " << tupleList[i].heapIndex << " TEST " << leftTableHeaps[tupleList[i].heapIndex].fileName << endl; } cout << endl;
        
        //START SORTIGN INTO ONE HEAP
        int emptyDBFileCount = 0;
        bool noMoreHeaps = false;
        while(leftTableHeaps.size() > emptyDBFileCount){
            pushMinimumLeft:
            vector<Tuple>::iterator min = min_element(tupleList.begin(), tupleList.end(), TupleComp());
            Record recordToStore = min.base()->rec;
            
            recordToStore.print(myfile,this->schemaLeft);
            myfile << endl;
            
            finalHeap.AppendRecord(recordToStore);
            counter++;
            
            if(noMoreHeaps) { tupleList.erase(min); }
            Record recordToReplenish;
            if(leftTableHeaps[min.base()->heapIndex].GetNext(recordToReplenish)){
                recordToReplenish.SetOrderMaker(leftOrder);
                Tuple addme; addme.heapIndex = min.base()->heapIndex; addme.rec = recordToReplenish;
                tupleList.erase(min);
                tupleList.push_back(addme);
            } else { 
                for(int i = 0; i < tupleList.size(); i++){ cout << "ERASE " << tupleList[i].heapIndex << " TEST " << leftTableHeaps[tupleList[i].heapIndex].fileName << endl; }
                //min.base()->rec.print(cout,schemaLeft); cout << endl;
                emptyDBFileCount++;  cout << "incrementing DBFile erase Count: " << emptyDBFileCount << " leftTableHeaps.size() = " << leftTableHeaps.size() << " NAME " << leftTableHeaps[min.base()->heapIndex].fileName << endl;
                tupleList.erase(min); }
        } if(tupleList.size() != 0) { 
            cout << "HEY" << endl; noMoreHeaps = true; goto pushMinimumLeft; }
        
        
        cout << "Total Inserted Into Final Heap: " << counter << endl;
        
        //CLEAN UP
        finalHeap.Close();
        for(int i = 0; i < leftTableHeaps.size();i++){ remove(leftTableHeaps[i].fileName.c_str()); }
        leftTableHeaps.clear();
        
        //PUSH BACK FINAL HEAP
        leftTableHeaps.push_back(finalHeap);
        myfile.close();
    } else {
        //CREATE FINAL HEAP
        DBFile finalHeap;
        string path = "Heaps//rhf_" + to_string(rand()) + ".dat";
        finalHeap.Create(&path[0], Sorted);
        
        //POPULATE REPLENISHER
        for(int i = 0; i < rightTableHeaps.size(); i++){
            if(!rightTableHeaps[i].isOpen) { rightTableHeaps[i].Open(); cout << "Opening Heap: " << rightTableHeaps[i].fileName << endl; }
            rightTableHeaps[i].MoveFirst();
            Record temp;
            if(rightTableHeaps[i].GetNext(temp)){
                temp.SetOrderMaker(rightOrder);
                Tuple tuple; tuple.heapIndex = i; tuple.rec = temp;
                tupleList.push_back(tuple);
            } else { cout << "Error" << endl; return -1; }
        }
        
        //START SORTIGN INTO ONE HEAP
        int emptyDBFileCount = 0;
        bool noMoreHeaps = false;
        while(rightTableHeaps.size() > emptyDBFileCount){
            pushMinimumRight:
            vector<Tuple>::iterator min = min_element(tupleList.begin(), tupleList.end(), TupleComp());
            Record recordToStore = min.base()->rec;
            
            recordToStore.print(myfile,this->schemaRight);
            myfile << endl;
            
            
            finalHeap.AppendRecord(recordToStore);
            counter++;
            
            
            if(noMoreHeaps) { tupleList.erase(min); }
            Record recordToReplenish;
            if(rightTableHeaps[min.base()->heapIndex].GetNext(recordToReplenish)){
                recordToReplenish.SetOrderMaker(leftOrder);
                Tuple addme; addme.heapIndex = min.base()->heapIndex; addme.rec = recordToReplenish;
                tupleList.erase(min);
                tupleList.push_back(addme);
            } else { 
                for(int i = 0; i < tupleList.size(); i++){ cout << "ERASE " << tupleList[i].heapIndex << " TEST " << rightTableHeaps[tupleList[i].heapIndex].fileName << endl; }
                //min.base()->rec.print(cout,schemaLeft); cout << endl;
                emptyDBFileCount++;  cout << "incrementing DBFile erase Count: " << emptyDBFileCount << " rightTableHeaps.size() = " << rightTableHeaps.size() << " NAME " << rightTableHeaps[min.base()->heapIndex].fileName << endl;
                tupleList.erase(min); }
        } if(tupleList.size() != 0) { 
            cout << "HEY" << endl; noMoreHeaps = true; goto pushMinimumRight; }
        
        
         cout << "Total Inserted Into Final Heap: " << counter << endl;
        
        //CLEAN UP
        for(int i = 0; i < rightTableHeaps.size();i++){ remove(rightTableHeaps[i].fileName.c_str()); }
        rightTableHeaps.clear();
        
        //PUSH BACK FINAL HEAP
        finalHeap.Close();
        rightTableHeaps.push_back(finalHeap);
        myfile.close();
    }
}
int Join::GenerateHeapPart(int& index, Table table){
    //GENERATING NAME
    ofstream myfile;
    string pathing = ((table == TableLeft) ? ("Heaps//lhp_" + to_string(index) + ".txt") : ("Heaps//rhp_" + to_string(index) + ".txt"));
    myfile.open(pathing);
        
    string path = ((table == TableLeft) ? ("Heaps//lhp_" + to_string(index) + ".dat") : ("Heaps//rhp_" + to_string(index) + ".dat"));
    
    cout << "Creating Heap: " << path << endl;
    
    int recordStored = 0;
    
    //CREATING HEAP
    DBFile newPartHeap;
    newPartHeap.Create(&path[0],Sorted);
    
    //SORTING
    sort(memoryTable.begin(),memoryTable.end(),RecordComp());
    cout << "-Sorted Memory sized: " << memoryTable.size() << endl;
    
    //STORING INTO HEAP_PART
    for(int i = 0; i < memoryTable.size(); i++) {  
        Schema ptr = ((table == TableLeft) ? schemaLeft : schemaRight);
        memoryTable.at(i).print(myfile,ptr); 
        myfile << endl; 
        newPartHeap.AppendRecord(memoryTable.at(i));
        recordStored++;}
    cout << "--Inserted records in Heap: " << recordStored << endl << endl;
    
    //CLEAN UP
    memoryTable.clear();
    memoryTable.shrink_to_fit();
    vector<Record> empty; memoryTable.swap(empty);
    //newPartHeap.MoveFirst();
    
    //ADD TO HEAPLIST
    newPartHeap.Close();
    ((table == TableLeft) ? leftTableHeaps : rightTableHeaps).push_back(newPartHeap);
    
    
    
    //TEST
    //newPartHeap.Close();
    //newPartHeap.Open(); //DO NOT REOPEN IT IT PRODUCES ONLY 1 PGAE
    //int count = 0;
    //Record ober;
    //while(newPartHeap.GetNext(ober)){
    //    count++;
    //} cout << "----COUNTED RECORDS: " << count << endl;
    //newPartHeap.MoveFirst();
    myfile.close();
    
    //RETURNING RECORDS STORED
    return recordStored;
}
int Join::LoadMoreLargerTable(){
    if(!((leftIsSmaller) ? rightTableHeaps[0].isOpen : leftTableHeaps[0].isOpen)) { 
        cout << "Opening Larger DBFile: " << ((leftIsSmaller) ? rightTableHeaps[0].fileName : leftTableHeaps[0].fileName) << endl; 
        ((leftIsSmaller) ? rightTableHeaps[0].Open() : leftTableHeaps[0].Open()); }
    memoryTableLarger.clear();
    Record record;
    int totalRecordMemorySize = 0, recordCount = 0;
    while((leftIsSmaller) ? rightTableHeaps[0].GetNext(record) : leftTableHeaps[0].GetNext(record)){
        record.SetOrderMaker((leftIsSmaller) ? rightOrder : leftOrder);
        memoryTableLarger.push_back(record);
        recordCount++;
        totalRecordMemorySize += record.GetSize();
        if(totalRecordMemorySize >= (noPages/2) * PAGE_SIZE){ break; }
    } return recordCount;
}
int Join::LoadMoreSmallerTable(){
    if(!((leftIsSmaller) ? leftTableHeaps[0].isOpen : rightTableHeaps[0].isOpen)) { 
        cout << "Opening Smaller DBFile: " << ((leftIsSmaller) ? leftTableHeaps[0].fileName : rightTableHeaps[0].fileName) << endl;
        ((leftIsSmaller) ? leftTableHeaps[0].Open() : rightTableHeaps[0].Open()); }
    memoryTable.clear();
    Record record;
    int totalRecordMemorySize = 0, recordCount = 0;
    while((leftIsSmaller) ? leftTableHeaps[0].GetNext(record) : rightTableHeaps[0].GetNext(record)){
        record.SetOrderMaker((leftIsSmaller) ? leftOrder : rightOrder);
        memoryTable.push_back(record); 
        recordCount++;
        totalRecordMemorySize += record.GetSize();
        if(totalRecordMemorySize >= (noPages/2) * PAGE_SIZE){ break;}
    } return recordCount;
}

struct PrevRecIndex{
    Record rec;
    int index;
};
Join::Join(int& numPages, Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {
        
    ///hope
    noPages = numPages;
    schemaLeft = _schemaLeft;
    schemaRight = _schemaRight;
    schemaOut = _schemaOut;
    predicate = _predicate;
    left = _left;
    right = _right;
    auto copyOut = new Schema();
    copyOut->Append(_schemaOut);
    
    //ORDER SETTING HERE
    leftOrder = new OrderMaker();
    rightOrder = new OrderMaker();
    _predicate.GetSortOrders(*leftOrder,*rightOrder);
    
    //CHECK IF FITTING IN MEMORY 
    cout << "Checking if in memory sort capable..." << endl;
    outOfMemory = false;
    unsigned int totalRecordMemorySize = 0;
    if(schemaRight.GetDistincts(schemaRight.atts.at(0).name) >= schemaLeft.GetDistincts(schemaLeft.atts.at(0).name)){
        
        cout << schemaLeft << endl;
        
        leftIsSmaller = true;
        largerTable = _right;
        Record temp;
        while(left->GetNext(temp)){
            temp.SetOrderMaker(leftOrder);
            totalRecordMemorySize += temp.GetSize();
            smallTable.Insert(temp);
            //memoryTable.push_back(temp);
            if(totalRecordMemorySize >= noPages * PAGE_SIZE){ outOfMemory = true; break; }
        }
    } else {
        
       cout << schemaRight << endl; 
        
       leftIsSmaller = false; 
       largerTable = _left;
       Record temp;
       while(right->GetNext(temp)){
            totalRecordMemorySize += temp.GetSize();
            temp.SetOrderMaker(rightOrder);
            smallTable.Insert(temp);
            //memoryTable.push_back(temp);
            if(totalRecordMemorySize >= noPages * PAGE_SIZE){ outOfMemory = true; break; }
        }
    }
    
    
    //CREATE HEAPS 
    if(outOfMemory){
        cout << "Starting out of memory algo..." << endl;
        Record record;
        //LOAD MEMORY FROM TWOWAYLIST INTO VECTOR
        smallTable.MoveToStart();
        while(smallTable.Length() != 0) { //empty old list and put into vector
                smallTable.Remove(record);
                record.SetOrderMaker( (leftIsSmaller) ? leftOrder : rightOrder );
                memoryTable.push_back(record);
        }
        
        
        int smallerTableCount = 0,
            largerTableCount = 0;
        
        //PRINTING SMALLER TABLE SCHEMA
        cout << ((leftIsSmaller) ? schemaLeft : schemaRight) << endl;
        
        //SMALLER TABLE
        int heap_index = 0;
        Table table;
        ((leftIsSmaller) ? table = TableLeft : table = TableRight);
        while((leftIsSmaller) ? left->GetNext(record) : right->GetNext(record)){
            if(totalRecordMemorySize >= noPages * PAGE_SIZE) { 
                totalRecordMemorySize = 0;
                smallerTableCount += GenerateHeapPart(++heap_index,table); 
            }
            record.SetOrderMaker((leftIsSmaller) ? leftOrder : rightOrder);
            memoryTable.push_back(record); 
            totalRecordMemorySize +=record.GetSize();
        } if(memoryTable.size() != 0) { smallerTableCount += GenerateHeapPart(++heap_index,table); }
        
        
        
        //GENERATE FINAL SMALLER HEAP
        GenerateFinalHeap(table);
        
        
        //PRINTING LARGER TABLE SCHEMA
        cout << ((leftIsSmaller) ? schemaRight : schemaRight) << endl;
        
        
        //LARGER TABLE
        totalRecordMemorySize = 0;
        heap_index = 0;
        
        ((leftIsSmaller) ? table = TableRight : table = TableLeft);
        while((leftIsSmaller) ? right->GetNext(record) : left->GetNext(record)){
            
            record.SetOrderMaker((leftIsSmaller) ? rightOrder : leftOrder);
            memoryTable.push_back(record); 
            totalRecordMemorySize +=record.GetSize();
            if(totalRecordMemorySize >= noPages * PAGE_SIZE) { 
                totalRecordMemorySize = 0;
                largerTableCount += GenerateHeapPart(++heap_index,table);
            }
        } if(memoryTable.size() != 0) { smallerTableCount += GenerateHeapPart(++heap_index,table); }
        
        
        
        //GENERATE FINAL SMALLER HEAP
        GenerateFinalHeap(table);
        
        
        //PERFORM 3-PASS
        cout << "JOIN RUNNING" << endl;
        //totalRecordMemorySize = 0;
        
        //CREATING JOIN HEAP
        string finalJoinPath = "Heaps//finalJoin_" + to_string(rand()) + ".dat";
        joinDBFile.Create(&finalJoinPath[0],Sorted);
        
        ofstream myfile;
        string finalJoinPathfile = "Heaps//finalJoin_" + to_string(rand()) + ".txt";
        myfile.open(finalJoinPathfile);
        
        while(LoadMoreSmallerTable() != 0){
            //PREVIOUS RECORD
            PrevRecIndex previousRecord; previousRecord.index = -1;
            
            bool atFirstLarger = true, firstTime = true;
            
            //ITERATE TRU SMALLER TABLE
            for(int i = 0; i < memoryTable.size(); i++){
                //CURRENT RECORD
                Record currentRecord = memoryTable.at(i);

                int countLarger = 0;
                countLarger = LoadMoreLargerTable();

                bool breakWhile = false;
                int joinDuplicateCount = 0;
                while(countLarger != 0){
                    for(int j = 0; j < memoryTableLarger.size(); j++){
                        Record fromLargerTable = memoryTableLarger.at(j);

                        //THIS MEANS DUPLICATE RECORD
                        if(false){//){(previousRecord.index != -1) && currentRecord.IsEqual(previousRecord.rec)){
                            if(previousRecord.index == 0){ //NO PREVIOUS MATCHES FOR THE SAME RECORD BREAK
                                //BREAK OUT OF WHILE
                                breakWhile = true;
                                //RESET LARGER HEAP
                                ((leftIsSmaller) ? rightTableHeaps[0].MoveFirst() : leftTableHeaps[0].MoveFirst()); 
                                // BREAK OUT OF FOR
                                break;
                            }  else {
                                // GO BACK "INDEX" MANY RECORDS IN LARGE TABLE
                                j -= previousRecord.index; 
                                if(j < 0) { cout << "!--THE DUPLICATE WAS IN PREVIOUS MEMORY BLOCK--!" << endl; j = 0;}
                                previousRecord.index = -1;
                                goto recordComparison;
                            }
                        } else { //ACTUAL JOINT 420
                            recordComparison:
                            if(currentRecord.LessThan(fromLargerTable)) { 
                                //BREAK OUT OF WHILE
                                breakWhile = true;
                                //RESET LARGER HEAP
                                ((leftIsSmaller) ? rightTableHeaps[0].MoveFirst() : leftTableHeaps[0].MoveFirst()); 
                                // BREAK OUT OF FOR
                                break; 
                            } else if (currentRecord.IsEqual(fromLargerTable)){
                                // SAVE PREVIOUS POSITION

                                if(!leftIsSmaller){
                                    
                                    // DO THE JOINt
                                    Record joinedRecord;
                                    if(predicate.Run(fromLargerTable,currentRecord)){
                                        //FIND PROPER SCHEMAS
                                        Schema SchemaLeft = ((leftIsSmaller) ? schemaLeft : schemaRight);
                                        Schema SchemaRight = ((leftIsSmaller) ? schemaRight : schemaLeft);
                                        //COMPUTE THE JOINT RECORD

                                        joinedRecord.AppendRecords(fromLargerTable,currentRecord, SchemaRight.atts.size(),SchemaLeft.atts.size());

                                        //TEST
                                        joinedRecord.print(myfile,this->schemaOut); myfile<<endl;


                                        //ADD TO JOIN HEAP FILE
                                        joinDBFile.AppendRecord(joinedRecord);

                                        //Increment the joinDuplicateCount
                                        joinDuplicateCount++;

                                    } else { /*cout << "ERROR WITH JOINING RECORDS" << endl;*/ }
                                } else {
                                    
                                    // DO THE JOINt
                                    Record joinedRecord;
                                    if(predicate.Run(currentRecord,fromLargerTable)){
                                        //FIND PROPER SCHEMAS
                                        Schema SchemaLeft = ((leftIsSmaller) ? schemaLeft : schemaRight);
                                        Schema SchemaRight = ((leftIsSmaller) ? schemaRight : schemaLeft);
                                        //COMPUTE THE JOINT RECORD

                                        joinedRecord.AppendRecords(currentRecord,fromLargerTable, SchemaLeft.atts.size(),SchemaRight.atts.size());

                                        //TEST
                                        joinedRecord.print(myfile,this->schemaOut); myfile<<endl;


                                        //ADD TO JOIN HEAP FILE
                                        joinDBFile.AppendRecord(joinedRecord);

                                        //Increment the joinDuplicateCount
                                        joinDuplicateCount++;

                                    } else { /*cout << "ERROR WITH JOINING RECORDS" << endl;*/ }
                                }
                            } else { /*DO NOTHING*/ }
                        }
                    } if(breakWhile){ countLarger = 0; } else { countLarger = LoadMoreLargerTable(); }
                }
                previousRecord.rec = currentRecord;
                previousRecord.index = joinDuplicateCount;
            }
        }
        // SAVE THE LAST PAGE
        joinDBFile.Close();
        myfile.close();
    // RESET THE MEMORY TABLE FOR IN MEMORY SORT
    } else {  smallTable.MoveToStart(); }
    
}



Join::~Join() {
}



bool Join::GetNext(Record& _record){
    // old
    if(!outOfMemory){
        while(true){
            if(smallTable.AtEnd()){ 
                if(!(largerTable->GetNext(curRecord))){ return false; } 
                else { ((leftIsSmaller) ? curRecord.SetOrderMaker(rightOrder) : curRecord.SetOrderMaker(leftOrder)); } 
                smallTable.MoveToStart(); 
            }
            while (!smallTable.AtEnd()){
                Record smallTableCurrent = smallTable.Current();
                ((leftIsSmaller) ? smallTableCurrent.SetOrderMaker(leftOrder) : smallTableCurrent.SetOrderMaker(rightOrder));
                if(leftIsSmaller){
                    if (predicate.Run(smallTableCurrent, curRecord)){
                        _record.AppendRecords(smallTableCurrent, curRecord, schemaLeft.atts.size(), schemaRight.atts.size());
                        smallTable.Advance(); return true;
                    }
                } else {
                    if (predicate.Run(curRecord,smallTableCurrent)){
                        _record.AppendRecords(curRecord, smallTableCurrent, schemaLeft.atts.size(), schemaRight.atts.size());
                        smallTable.Advance(); return true;
                    }
                } smallTable.Advance();
            }
        } return false;
    }
    else {
        if(joinDBFile.isOpen){ joinDBFile.GetNext(_record); }
        else { joinDBFile.Open(); joinDBFile.GetNext(_record); }
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

bool DuplicateRemoval::GetNext(Record& _record){
    while(producer->GetNext(_record)){
        stringstream key;
        _record.print(key, schema);
        auto it = dupMap.find(key.str());
        if(it == dupMap.end()){
            dupMap[key.str()] = 1;
            return true;
        }
    }
    return false;
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
        alreadyCalculatedSum = false;
}

Sum::~Sum() {
}

bool Sum::GetNext(Record& _record){
    if(alreadyCalculatedSum){ return false; }
    int integer_sum = 0;
    double double_sum = 0;
    while(producer->GetNext(_record)){
        int integer_result = 0;
        double double_result = 0;
        
        (compute.Apply(_record,integer_result,double_result) == Integer)?
        integer_sum += integer_result:
        double_sum += double_result;
        
    }
    double sum_result = (double)integer_sum + double_sum; // one of them will be zero
    

    char* recSpace = new char[16];
    int currentPosInRec = sizeof (int) * (2);
    ((int *) recSpace)[1] = currentPosInRec;
    
    if(schemaOut.GetAtts()[0].type == Integer){
            *((int *) (recSpace + currentPosInRec)) = sum_result;
    }else{
            *((double *) (recSpace+ currentPosInRec)) = sum_result;
    }
    
    //*((double *) (recSpace+ currentPosInRec)) = sum_result;
    currentPosInRec += sizeof (double);
    ((int *) recSpace)[0] = currentPosInRec;
    
    Record sumRec;
    
    
    //sumRec.CopyBits( recSpace, currentPosInRec );
    
    //delete [] recSpace;
    
    //cout<<((int*) recSpace)[0]<<endl;
    //cout<<((int*) recSpace)[1]<<endl;
    //cout<<*((double*) (recSpace+8))<<endl;
    
    
    _record.Consume(recSpace);
    alreadyCalculatedSum = true;
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
	groupingAtts.Swap(_groupingAtts);
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

/*bool GroupBy::GetNext(Record& _record){
    if(!mapsCreated){
        int integer_sum = 0;
        double double_sum = 0;
        while(producer->GetNext(_record)){
            stringstream key;
            int integer_result = 0;
            double double_result = 0;

            (compute.Apply(_record,integer_result,double_result) == Integer)?
            integer_sum += integer_result:
            double_sum += double_result;
            double sum_result = (double)integer_sum + double_sum; // one of them will be zero
            
            _record.Project(&groupingAtts.whichAtts[0], groupingAtts.numAtts , schemaOut.GetNumAtts());
            _record.print(key, schemaOut);
            auto it = sumMap.find(key.str());

            if(it != sumMap.end())	{ sumMap[key.str()]+= sum_result; }
            else {
                    sumMap[key.str()] = sum_result;
                    recordMap[key.str()] = _record;
            }

        }
        mapsCreated = true;
    } else {
        if (sumMap.empty()) return false;

        Record temp = recordMap.begin()->second;
        string topr = sumMap.begin()->first;

        char* recSpace = new char[16];
        int currentPosInRec = sizeof (int) * (2);
        ((int *) recSpace)[1] = currentPosInRec;
        *((double *) &(recSpace[currentPosInRec])) = sumMap.begin()->second;
        currentPosInRec += sizeof (double);
        ((int *) recSpace)[0] = currentPosInRec;
        Record sumRec;
        sumRec.CopyBits( recSpace, currentPosInRec );
        delete [] recSpace;

        Record newRec;
        newRec.AppendRecords(sumRec, temp, 1, schemaOut.GetNumAtts()-1);
        recordMap.erase(topr);
        sumMap.erase(topr);
        _record = newRec;
        return true;
    }
}*/

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
