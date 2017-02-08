#include <iostream>

#include "Schema.h"
#include "Catalog.h"

using namespace std;
namespace extensions
{
    template < typename T > string to_string( const T& n ){
        ostringstream stm; stm << n; return stm.str() ;
    }
}


Catalog::Catalog(string& _fileName) {
    //open database
    int rc = sqlite3_open_v2(_fileName.c_str(), &_db, SQLITE_OPEN_READWRITE, 0);
    if(rc == SQLITE_OK){ _dbOpen = 1; } else { _dbOpen = 0; }
    //read database
    if(_dbOpen){ ReadDatabase(); }
}

Catalog::~Catalog() {
}

bool Catalog::Save() {
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
    Keyify<string> key(_table);
    if(catalog_tbl.IsThere(key)){
        Schema s = catalog_tbl.Find(key);
        _noTuples = s._noTuples;
        return true;
    } else return false;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
    Keyify<string> key(_table);
    if(catalog_tbl.IsThere(key)){
        Schema s = catalog_tbl.Find(key);
        s._noTuples = _noTuples;
        
        Keyify<string> key(_table);
        vector<string> a;
        vector<string> b;
        vector<unsigned int> c;
        Schema empty(a,b,c);
        Swapify<Schema> ss(empty);
        if(catalog_tbl.Remove(key,key,ss)){
            s._noTuples = _noTuples;
            Swapify<Schema> sol(s);
            catalog_tbl.Insert(key,sol);
        }
    }
}

bool Catalog::GetDataFile(string& _table, string& _path) {
    Keyify<string> key(_table);
    if(catalog_tbl.IsThere(key)){
        Schema s = catalog_tbl.Find(key);
        _path = s._location;
        return true;
    } else return false;
}

void Catalog::SetDataFile(string& _table, string& _path) {
    Keyify<string> key(_table);
    if(catalog_tbl.IsThere(key)){
        Schema s = catalog_tbl.Find(key);
        
        Keyify<string> key(_table);
        vector<string> a;
        vector<string> b;
        vector<unsigned int> c;
        Schema empty(a,b,c);
        Swapify<Schema> ss(empty);
        if(catalog_tbl.Remove(key,key,ss)){
            s._location = _path;
            Swapify<Schema> sol(s);
            catalog_tbl.Insert(key,sol);
        }
    }
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,unsigned int& _noDistinct) {
    Keyify<string> key(_table);
    if(catalog_tbl.IsThere(key)){
        Schema s = catalog_tbl.Find(key);
        vector<Attribute> a = s.GetAtts();
        int size = a.size();
        for(int i = 0; i < size; i++){
            if(_attribute == s.GetAtts().at(i).name){
                _noDistinct = s.GetAtts().at(i).noDistinct;
                return true;
            }
        }
        //this means the table did not have attribute with that name
        return false; // shud we have this here?
    } else return false;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct) {
    Keyify<string> key(_table);
    if(catalog_tbl.IsThere(key)){
        Schema s = catalog_tbl.Find(key);
        Swapify<Schema> ss(s);
        catalog_tbl.Remove(key,key,ss);
        for(int i = 0; i < s.GetAtts().size(); i++){
            if(_attribute == s.GetAtts().at(i).name){
                s.GetAtts().at(i).noDistinct = _noDistinct;
            }
        }
        ss = Swapify<Schema>(s);
        catalog_tbl.Insert(key,ss);
    }
}

void Catalog::GetTables(vector<string>& _tables) {
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
	return true;
}

bool Catalog::DropTable(string& _table) {
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}

bool Catalog::ReadDatabase(){
    try{
        if(_dbOpen){
            sqlite3_stmt *stmt; string query = "SELECT * FROM Catalog";
            if(sqlite3_prepare(_db,query.c_str(),-1,&stmt,0) == SQLITE_OK){
                int ctotal = sqlite3_column_count(stmt);
                int rc = 0;
                while(1){
                    rc = sqlite3_step(stmt);
                    
                    string tableName;
                    int noTuples = 0;
                    string location;
                    vector<string> attrbNames;
                    vector<string> attrbTypes;
                    vector<unsigned int> attrbDistinct;
                    Schema s;
                    
                    if(rc == SQLITE_ROW){
                        for(int i = 0; i < ctotal; i++){
                            switch(i){
                                case 0:{ // catalog.tableName
                                    tableName = (char*)sqlite3_column_text(stmt,i);
                                    break;
                                }
                                case 1:{ // catalog.numTuples
                                    noTuples = sqlite3_column_int64(stmt,i);
                                    break;
                                }
                                case 2:{ // catalog.location
                                    location = (char*)sqlite3_column_text(stmt,i);
                                    break;
                                }
                            };
                        }
                        sqlite3_stmt *att_stmt; string query2 = "SELECT * FROM Attribute WHERE tableName = '" + tableName +"'";
                        if(sqlite3_prepare(_db,query2.c_str(),-1,&att_stmt,0) == SQLITE_OK){
                            int ctotal1 = sqlite3_column_count(att_stmt);
                            int rc1 = 0;
                            while(1){
                                rc1 = sqlite3_step(att_stmt);
                                if(rc1 == SQLITE_ROW){
                                    string name;
                                    string tableName;
                                    string type;
                                    int distinctTuple = 0;
                                    for(int i = 0; i < ctotal1; i++){
                                        switch(i){
                                            case 0:{ // attribute.name
                                                name = (char*)sqlite3_column_text(att_stmt,i);
                                                attrbNames.push_back(name);
                                                break;
                                            }
                                            case 1:{ // attribute.tableName
                                                tableName = (char*)sqlite3_column_text(att_stmt,i);
                                                break;
                                            }
                                            case 2:{ // attribute.type
                                                type = (char*)sqlite3_column_text(att_stmt,i);
                                                attrbTypes.push_back(type);
                                                break;
                                            }
                                            case 3:{ // attribute.distinctTuple
                                                distinctTuple = sqlite3_column_int64(att_stmt,i);
                                                attrbDistinct.push_back(distinctTuple);
                                                break;
                                            }
                                        };
                                    }
                                }
                                if(rc1 == SQLITE_ERROR){ return 0; }
                                if(rc1 == SQLITE_DONE){ break; }
                            }
                        } sqlite3_finalize(att_stmt);
                        s = Schema(attrbNames,attrbTypes,attrbDistinct);
                        s._nameTable = tableName;
                        s._location = location;
                        s._noTuples = noTuples;
                        Keyify<string> key(tableName);
                        Swapify<Schema> data(s);
                        catalog_tbl.Insert(key,data);
                    }
                    if(rc == SQLITE_ERROR){ return 0; }
                    if(rc == SQLITE_DONE){ break; }
                }
            } sqlite3_finalize(stmt);
        } else return 0;
    } catch (const char* msg) { return 0; }
    return 1;
}
bool Catalog::WriteDatabse(){
    
}
