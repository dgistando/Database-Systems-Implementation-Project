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
	OptimizationTree* _root) {
    vector<mapkey> mapKey;
    //assume complete optimize algorithm exits 
    int index = 0;
    TableList * _tempTables = _tables;

    CNF _cnf;
    while(_tempTables != NULL){
        string _tableName = _tempTables->tableName;

        Schema _schema;
        catalog->GetSchema(_tableName, _schema);

        unsigned int numTup;
        catalog->GetNoTuples(_tableName, numTup);
        string _key = extensions::to_string(index);
        initial[_key].size = numTup; 
        initial[_key].cost = 0;
        initial[_key].schema = _schema;

        final[_key] = _tableName;

        //Schema _schema; Its above
        Record _record;
        unsigned long long _div = 0;
        //catalog->GetSChema(tableName,_schema); Its above
        if(_cnf.ExtractCNF(*_predicate,_schema,_record) == 0){
            if(_cnf.numAnds > 0){
                for(int i = 0; i < _cnf.numAnds;i++){
                    if(_cnf.andList[i].operand1 == Left || _cnf.andList[i].operand1 == Right){
                        if(_cnf.andList[i].op == LessThan || _cnf.andList[i].op == GreaterThan){ _div = 3; }
                        else{
                        vector<Attribute> _atts;
                        _atts = _schema.GetAtts();
                        _div = _atts[_cnf.andList[i].whichAtt1].noDistinct;
                    }
                }
                if(_cnf.andList[i].operand2 == Left || _cnf.andList[i].operand2 == Right){
                    if(_cnf.andList[i].op == LessThan || _cnf.andList[i].op == GreaterThan){ _div = 3; }
                    else{
                        vector<Attribute> _atts;
                        _atts = _schema.GetAtts();
                        _div = _atts[_cnf.andList[i].whichAtt2].noDistinct;
                    }
                }
                }
                initial[_key].cost /= _div;
            }
        }
        
        mapkey node;
        node.tableName = _tableName;
        node.key = _key;
        mapKey.push_back(node);
        
        index++;
        _tempTables = _tempTables->next;
    }
    cout << "check" << endl;
    _tempTables = _tables; int check = 0;
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
                _attr2 = _schema.GetAtts();
                int i = 0; string _key = "";
                while(1){ if(_tableName == mapKey.at(i).tableName) { _key += mapKey.at(i).key; _tupsOne = initial[mapKey.at(i).key].cost; i = 0; break; } i++;}
                while(1){ if(_tableNameTwo == mapKey.at(i).tableName) { _key += mapKey.at(i).key; _tupsTwo = initial[mapKey.at(i).key].cost; i = 0; break; } i++;}

                if(_cnf.numAnds > 0) { 
                    for (int i = 0 ; i < _cnf.numAnds; i++)
                    {
                        if (_cnf.andList[i].operand1 == Left)
                        {
                                _noDisOne = _attr1[_cnf.andList[i].whichAtt1].noDistinct;
                                _noDisTwo = _attr2[_cnf.andList[i].whichAtt2].noDistinct;
                        }

                        if (_cnf.andList[i].operand1 == Right)
                        {
                                _noDisOne = _attr1[_cnf.andList[i].whichAtt2].noDistinct;
                                _noDisTwo = _attr2[_cnf.andList[i].whichAtt1].noDistinct;
                        }

                        if (_noDisOne > _noDisTwo){ _div = _noDisOne; } else { _div = _noDisTwo; }
                    }
                }
                if (_cnf.numAnds == 0) { _div = 1; }
                initial[_key].size = (_tupsOne*_tupsTwo)/_div;
                initial[_key].cost = 0;
                initial[_key].key = _key;
                mapkey node;
                node.key = _key;
                node.tableName = _tableName + "," + _tableNameTwo;
                mapKey.push_back(node);
            }
           _tempTablesTwo = _tempTablesTwo->next;
        }
        _tempTables = _tempTables->next;
    }
    string keys = "";
    for(int i = 0; i < mapKey.size(); i++){ keys += mapKey.at(i).key; }
    Partition(keys);
    
    _root = new OptimizationTree;
    _root -> leftChild = NULL;
    _root -> rightChild = NULL;
    for (int i = 0; i < keys.size(); i++) 
    {
            _root -> tables.push_back(final[ {keys[i]} ] );
    }

    cout<<endl<<endl;
    _root -> noTuples = initial[keys].size;
    keys = initial[keys].key;

    treeGenerator(keys, _root);

    cout<<"\n\nDisplaying Tree\n\n";
    treeDisp(_root);
}
void QueryOptimizer::treeDisp(OptimizationTree* & root)
{

    if (root -> leftChild == NULL && root -> rightChild == NULL) 
    {
            cout<<root -> tables[0]<<endl;		
            cout<<endl;
            return;
    }
    treeDisp(root->leftChild);
    treeDisp(root->rightChild);

}

void QueryOptimizer::Partition(string _tableIndecies){
    string copy = _tableIndecies;
    unsigned long long min_cost = LLONG_MAX,cost,size;
    string order;

    sort(copy.begin(), copy.end());

    map<string,opt>::iterator it = initial.find(copy);
    if(it != initial.end()) return;

    int N = _tableIndecies.size();
    bool lastPerm = true;

    while (lastPerm)
    {	
            for (int j = 0; j <= N - 2; j++)
            {
                    string left="";

                    for (int ind = 0; ind <= j; ind++) left+= _tableIndecies[ind];
                    Partition(left);

                    string right="";

                    for (int ind = j+1; ind < N; ind++) right+= _tableIndecies[ind];
                    Partition(right);

                    sort(left.begin(), left.end());
                    sort(right.begin(), right.end());

                    cost = initial[left].cost + initial[right].cost;

                    if (j!=0) cost += initial[left].size;
                    if (j!=N-2) cost += initial[right].size;

                    if (cost < min_cost){
                            size = initial[left].size*initial[right].size;
                            //order = "(" + Map[left].order + "," + Map[right].order + ")";
                            order = initial[left].key + "," + initial[right].key;
                            min_cost = cost;
                    }
            }

            lastPerm = Permute(_tableIndecies);
    }

    initial[copy].key = order;
    initial[copy].size = size;
    initial[copy].cost = min_cost;
}
void QueryOptimizer::treeGenerator(string tabList, OptimizationTree* & root)
{
	string left,right;
	int pos = tabList.find(",");

	if (pos != string::npos)
	{
		left = tabList.substr(0,pos);
		right = tabList.substr(tabList.find(",")+1);

		cout<<"left  "<<left<<"   right   "<<right<<endl;
		root -> leftChild = new OptimizationTree;
		treeGenerator(left, root -> leftChild);
		root -> rightChild = new OptimizationTree;
		treeGenerator(right, root -> rightChild);
		return;
		
	}
		
	for (int i=0; i<tabList.size(); i++)
		root -> tables.push_back(final[ {tabList[i]} ]);
	
	if (tabList.size() == 1)
	{
		root -> noTuples = initial[tabList].size;
		root -> leftChild = NULL;
		root -> rightChild = NULL;
		return;
	}
	
	root -> leftChild = new OptimizationTree;
	root -> leftChild -> leftChild = NULL;
	root -> leftChild -> rightChild = NULL;
	root -> leftChild -> tables.push_back(root -> tables[0]);
	root -> leftChild -> noTuples = initial[{tabList[0]}].size;
	
	root -> rightChild = new OptimizationTree;
	root -> rightChild -> leftChild = NULL;
	root -> rightChild -> rightChild = NULL;
	root -> rightChild -> tables.push_back(root -> tables[1]);
	root -> rightChild -> noTuples = initial[{tabList[1]}].size;

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
