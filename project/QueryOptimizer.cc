#include <string>
#include <vector>
#include <iostream>
#include <climits>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"

using namespace std;

QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
	catalog = &_catalog;
}

QueryOptimizer::~QueryOptimizer() {

}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree*& _root) {
    InitializeMaps(_tables);
    cout << "\n\t--Initialized Maps--\n" << endl;
    PrintTables();
    CalculateSizeForOriginalTables(_tables,_predicate);
    cout << "\n\t--Calculated Size For Original Tables--\n" << endl;
    PrintTables();
    CalculateSizeForTableCombination(_tables,_predicate);
    cout << "\n\t--Calculated Size For Table Combinations--\n" << endl;
    PrintTables();
    
    string keys = "";
    for(int i = 0; i < mapKey.size(); i++){ if(tableOptMap[mapKey.at(i).key].singleTable) {keys += mapKey.at(i).key; }}
    Partition(keys);
        cout << "\n\t--Partition Algo Executed--\n" << endl;
    PrintTables();
    _root = new OptimizationTree;
    _root -> leftChild = NULL;
    _root -> rightChild = NULL;
    for (int i = 0; i < keys.size(); i++)  { _root -> tables.push_back(tableNamesMap[ {keys[i]} ] ); }
    _root -> noTuples = tableOptMap[keys].size;
    keys = tableOptMap[keys].key;
    
    cout << "\n\t--Rebuilding the Tree--\n" << endl;
    RegenerateTree(keys, _root);
    cout << "\n\t--Printing the Join Order--\n" << endl;
    PrintOptimizationTree(_root);
    _root = _root;
}
void QueryOptimizer::InitializeMaps(TableList* _tables){
    int index = 0;
    TableList * _tempTables = _tables;
    while(_tempTables != NULL){
        string _tableName = _tempTables->tableName;

        Schema _schema;
        catalog->GetSchema(_tableName, _schema);
        string _key = extensions::to_string(index);
        tableOptMap[_key].size = _schema._noTuples; 
        tableOptMap[_key].cost = 0;
        tableOptMap[_key].schema = _schema;
        tableOptMap[_key].key = _key;
        tableOptMap[_key].singleTable = true;

        tableNamesMap[_key] = _tableName;
        
        mapkey node;
        node.tableName = _tableName;
        node.key = _key;
        mapKey.push_back(node);
        
        index++;
        _tempTables = _tempTables->next;
    }
}
void QueryOptimizer::CalculateSizeForOriginalTables(TableList* _tables, AndList* _predicate){
    TableList * _tempTables = _tables;
    CNF _cnf;
    while(_tempTables != NULL){
        Schema _schema;
        Record _record;
        int _div = 1;
        string tableName(_tempTables->tableName);
        catalog->GetSchema(tableName,_schema);
        if(_cnf.ExtractCNF(*_predicate,_schema,_record) == 0){
            if(_cnf.numAnds > 0){ // shud be just 1 Ands for this cnf
                for(int i = 0; i < _cnf.numAnds;i++){
                    if(_cnf.andList[i].operand1 == Left || _cnf.andList[i].operand1 == Right){
                        if(_cnf.andList[i].op == LessThan || _cnf.andList[i].op == GreaterThan){ _div = 3; }
                        else{ _div = _schema.GetAtts()[_cnf.andList[i].whichAtt1].noDistinct; }
                    }
                    if(_cnf.andList[i].operand2 == Left || _cnf.andList[i].operand2 == Right){
                        if(_cnf.andList[i].op == LessThan || _cnf.andList[i].op == GreaterThan){ _div = 3; }
                        else{ _div = _schema.GetAtts()[_cnf.andList[i].whichAtt2].noDistinct; }
                    }   // map the size
                } int i = 0; while(1){ if(tableName == mapKey.at(i).tableName){ tableOptMap[extensions::to_string(i)].size /= _div; break; } i++; }
            }
        }
        _tempTables = _tempTables->next;
    }
}
void QueryOptimizer::PrintTables(){
        cout<<"\n\t-Table List:-\n";
	for (int i=0; i<mapKey.size(); i++) cout<<"TableName: "<<mapKey.at(i).tableName<<"\tMapKey: "<<mapKey.at(i).key<<endl;
	cout<<endl;
        cout<<"-\t-SIZE-\t\t\t-COST-"<<endl;
	for (std::map<string, opt>::iterator it=tableOptMap.begin(); it!=tableOptMap.end(); ++it)
	{
		cout<<it->first<<"-\t-"<<it->second.size<<"-\t\t-"<<it->second.cost<<"-"<<endl;
	}

	cout<<"\n------------------------------------------------\n";
}
void QueryOptimizer::CalculateSizeForTableCombination(TableList* _tables, AndList* _predicate){
    TableList * _tempTables = _tables; int check = 0;
    CNF _cnf;
    while (_tempTables->next != NULL){
        if (check == 0 && _tempTables->next == NULL) { break; } check = 1;
        Schema _schema;
	int _div = 0;
        string _tableName(_tempTables->tableName);
        catalog->GetSchema(_tableName, _schema);
        
        TableList * _tempTablesTwo = new TableList();
        _tempTablesTwo = _tempTables->next;//next table
        while(_tempTablesTwo != NULL){
            Schema _schemaTwo;
            unsigned long long _tupsOne, _tupsTwo, _noDisOne, _noDisTwo;
            string _tableNameTwo = _tempTablesTwo->tableName;
            catalog->GetSchema(_tableNameTwo,_schemaTwo);

            if(_cnf.ExtractCNF(*_predicate,_schema,_schemaTwo) == 0){
                vector<Attribute> _attr1,_attr2;
                _attr1 = _schema.GetAtts();
                _attr2 = _schemaTwo.GetAtts();
                int i = 0; string _key = "";
                while(1){ if(_tableName == mapKey.at(i).tableName) {  _key += mapKey.at(i).key;   _tupsOne = tableOptMap[mapKey.at(i).key].size;  break; } i++;} i = 0;
                while(1){ if(_tableNameTwo == mapKey.at(i).tableName) {  _key += mapKey.at(i).key; _tupsTwo = tableOptMap[mapKey.at(i).key].size; break; } i++;}
                if(_cnf.numAnds > 0) { 
                    for (int i = 0 ; i < _cnf.numAnds; i++) {
                        if (_cnf.andList[i].operand1 == Left)  {
                            _noDisOne = _attr1[_cnf.andList[i].whichAtt1].noDistinct;
                            _noDisTwo = _attr2[_cnf.andList[i].whichAtt2].noDistinct;
                        }

                        if (_cnf.andList[i].operand1 == Right) {
                            _noDisOne = _attr1[_cnf.andList[i].whichAtt2].noDistinct;
                            _noDisTwo = _attr2[_cnf.andList[i].whichAtt1].noDistinct;
                        }
                        if (_noDisOne > _noDisTwo){ _div = _noDisOne; } else { _div = _noDisTwo; }
                    }
                }
                if (_cnf.numAnds == 0) { _div = 1; }
                tableOptMap[_key].size = (_tupsOne * _tupsTwo)/_div;
                tableOptMap[_key].cost = 0;
                tableOptMap[_key].key = _key;
                mapkey node;
                node.key = _key;
                node.tableName = _tableName + "," + _tableNameTwo;
                mapKey.push_back(node);
            }
           _tempTablesTwo = _tempTablesTwo->next;
        }
        _tempTables = _tempTables->next;
    }
}
void QueryOptimizer::PrintOptimizationTree(OptimizationTree* & _root)
{

    if (_root -> leftChild == NULL && _root -> rightChild == NULL) { cout<<"TableName: "<<_root -> tables[0]<<" -JOIN- "; return; }
    PrintOptimizationTree(_root->leftChild);
    PrintOptimizationTree(_root->rightChild);

}
void QueryOptimizer::Partition(string _tableIndecies){
    string _copyIndecies = _tableIndecies;
    unsigned long long min_cost = LLONG_MAX,
            cost,
            size;
    string key;

    sort(_copyIndecies.begin(), _copyIndecies.end());

    map<string,opt>::iterator it = tableOptMap.find(_copyIndecies);
    if(it != tableOptMap.end()) return;

    int N = _tableIndecies.size();
    bool lastPerm = true;

    while (lastPerm)
    {	
            for (int j = 0; j <= N - 2; j++)
            {
                    string left=""; for (int ind = 0; ind <= j; ind++) left+= _tableIndecies[ind]; Partition(left);
                    string right=""; for (int ind = j+1; ind < N; ind++) right+= _tableIndecies[ind]; Partition(right);
                    sort(left.begin(), left.end());
                    sort(right.begin(), right.end());
                    cost = tableOptMap[left].cost + tableOptMap[right].cost;
                    if (j!=0) cost += tableOptMap[left].size;
                    if (j!=N-2) cost += tableOptMap[right].size;
                    if (cost < min_cost){
                            size = tableOptMap[left].size*tableOptMap[right].size;
                            key = tableOptMap[left].key + "," + tableOptMap[right].key;
                            min_cost = cost;
                    }
            }
            lastPerm = Permute(_tableIndecies);
    }
    tableOptMap[_copyIndecies].key = key;
    tableOptMap[_copyIndecies].size = size;
    tableOptMap[_copyIndecies].cost = min_cost;
}
void QueryOptimizer::RegenerateTree(string _tableIndecies, OptimizationTree*& _root){
    RegenerateLeaf(_tableIndecies,_root);
}
void QueryOptimizer::RegenerateLeaf(string _tableIndecies, OptimizationTree* & _root)
{
	string left,right;
	int pos = _tableIndecies.find(",");
        //string not found
	if (pos != string::npos) {
		left = _tableIndecies.substr(0,pos);
		right = _tableIndecies.substr(_tableIndecies.find(",")+1);
                cout<<"-----------------------------------------"<<endl;
		cout<<"\t-Left Leaf- "<<left<<" -Right Leaf- "<<right<<endl;
                cout<<"-----------------------------------------"<<endl;
		_root -> leftChild = new OptimizationTree;
		RegenerateLeaf(left, _root -> leftChild);
		_root -> rightChild = new OptimizationTree;
		RegenerateLeaf(right, _root -> rightChild);
		return;
	}
		
	for (int i=0; i<_tableIndecies.size(); i++){ _root -> tables.push_back(tableNamesMap[ {_tableIndecies[i]} ]); }
	if (_tableIndecies.size() == 1) {
		_root -> noTuples = tableOptMap[_tableIndecies].size;
		_root -> leftChild = NULL;
		_root -> rightChild = NULL;
		return;
	}
	_root -> leftChild = new OptimizationTree;
	_root -> rightChild = new OptimizationTree;
	_root -> leftChild -> leftChild = NULL;
	_root -> leftChild -> rightChild = NULL;
	_root -> rightChild -> leftChild = NULL;
	_root -> rightChild -> rightChild = NULL;
	_root -> leftChild -> tables.push_back(_root -> tables[0]);
	_root -> leftChild -> noTuples = tableOptMap[{_tableIndecies[0]}].size;
	_root -> rightChild -> tables.push_back(_root -> tables[1]);
	_root -> rightChild -> noTuples = tableOptMap[{_tableIndecies[1]}].size;

}
bool QueryOptimizer::Permute(string& array)
{
    int k = -1, l = 0;
    for (int i = 0; i < array.size()-1; i++)
            if(array[i]<array[i+1]) k = i;	

    if (k == -1) return false;		

    for (int i = k+1; i < array.size(); i++)
            if(array[k]<array[i]) l = i;	

    swap(array[k],array[l]);

    int count = 1, temp = k+1;
    for (int i=0; i<(array.size()-k-1)/2; i++,count++,temp++)
            swap(array[temp],array[array.size()-count]);

    return true;
}
int QueryOptimizer::Factorial(int n){if(n < 0) { return 0; } return !n ? 1 : n * Factorial(n - 1); }
void QueryOptimizer::Permute(vector<vector<string> >& output, vector<string> bin,int size_bin){
    if(size_bin == 1){ output.push_back(bin); return; }
    for(int i = 0; i < size_bin - 1; i ++){
        Permute(output,bin,size_bin -1);
        if(size_bin % 2 == 1) { swap(bin.at(0),bin.at(size_bin-1)); }
        else { swap(bin.at(i),bin.at(size_bin-1)); }
    }
}
