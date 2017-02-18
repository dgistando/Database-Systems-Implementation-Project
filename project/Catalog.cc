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

//Making sure its for my branch

Catalog::Catalog(string& _fileName) {
    //open database
    int rc = sqlite3_open_v2(_fileName.c_str(), &_db, SQLITE_OPEN_READWRITE, 0);
    if(rc == SQLITE_OK){ _dbOpen = 1; } else { _dbOpen = 0; }
    //read database
    if(_dbOpen){ ReadDatabase(); }
    else cout << "Cannot open db!" << endl;
}

Catalog::~Catalog() {
    WriteDatabse();
}

bool Catalog::Save() {
    WriteDatabse();
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
        
        Keyify<string> key(_table);
        vector<string> a;
        vector<string> b;
        vector<unsigned int> c;
        Schema empty(a,b,c);
        Swapify<Schema> ss(empty);
        
        if(catalog_tbl.Remove(key,key,ss)){
            s._noTuples = _noTuples;
            s._edited = true;
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
            s._edited = true;
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
        
        Keyify<string> key(_table);
        vector<string> a;
        vector<string> b;
        vector<unsigned int> c;
        Schema empty(a,b,c);
        Swapify<Schema> ss(empty);
        
        catalog_tbl.Remove(key,key,ss);
        for(int i = 0; i < s.GetAtts().size(); i++){
            if(_attribute == s.GetAtts().at(i).name){
                s.GetAtts().at(i).noDistinct = _noDistinct;
                s._edited = true;
                Swapify<Schema> sol(s);
                catalog_tbl.Insert(key,sol);
            }
        }
    }
}

void Catalog::GetTables(vector<string>& _tables) {
    catalog_tbl.MoveToStart();
    while(!catalog_tbl.AtEnd()){
        _tables.push_back(catalog_tbl.CurrentKey().operator string()); 
        catalog_tbl.Advance();
    }
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
    Keyify<string> key(_table);
    if(catalog_tbl.IsThere(key)){
        Schema s = catalog_tbl.Find(key).operator Schema();
        for(int i = 0; i < s.GetAtts().size(); i++){
            _attributes.push_back(s.GetAtts().at(i).name);
        }
        return true;
    } else return false;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
    Keyify<string> key(_table);
    if(catalog_tbl.IsThere(key)){
        _schema = catalog_tbl.Find(key).operator Schema();
        return true;
    } return false;
}

/* To do check if the attributes are unique
 */
bool Catalog::CreateTable(string& _table, vector<string>& _attributes,vector<string>& _attributeTypes) {
    Keyify<string> key(_table);
    if(_attributes.size() == 0){ return false; } // u shall not pass need more attrb
    if(!catalog_tbl.IsThere(key)){
        vector<unsigned int> d;
        for(int i = 0; i < _attributes.size(); i++){ d.push_back(0); }
        Schema s(_attributes,_attributeTypes,d);
        s._noTuples = 0;
        s._location = "";
        s._nameTable = _table;
        s._toCreate = true;
        s._edited = false;
        Swapify<Schema> ss(s);
        catalog_tbl.Insert(key,ss);
        return true;
    } else return false;
}

bool Catalog::DropTable(string& _table) {
    Keyify<string> key(_table);
    if(catalog_tbl.IsThere(key)){
        Schema s = catalog_tbl.Find(key);
        
        if(!s._toCreate){
            s._toCreate = false;
            Swapify<string> data(_table);
            tables_toDrop.Insert(key,data);
        }
        
        Keyify<string> key(_table);
        vector<string> a;
        vector<string> b;
        vector<unsigned int> c;
        Schema empty(a,b,c);
        Swapify<Schema> ss(empty);
        if(catalog_tbl.Remove(key,key,ss)) { return true; }
        else return false;
    } else return false;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
    _c.catalog_tbl.MoveToStart();
    while(!_c.catalog_tbl.AtEnd()){
        string str = "";
        Schema s = _c.catalog_tbl.CurrentData().operator Schema();
        str += ("\n" + s._nameTable + "\t" + extensions::to_string(s._noTuples) + "\t" + s._location + "\n");
        vector<string> bin;
        for(int i = 0; i < s.GetAtts().size(); i++){
            Attribute a = s.GetAtts().at(i);
            bin.push_back("\t" + a.name + "\t" + _c.ParseType(a.type) + "\t" + extensions::to_string(a.noDistinct) + "\n");
        }
        //sort(bin.begin(),bin.end());
        for(int i = 0; i < bin.size(); i++){ str += bin.at(i); }
        _c.catalog_tbl.Advance();
         _os << str;
    }
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
                        s._toCreate = false;
                        s._edited = false;
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
    if(_dbOpen){
        DeleteTables();
        catalog_tbl.MoveToStart();
        while(!catalog_tbl.AtEnd()){
            Schema s = catalog_tbl.CurrentData().operator Schema();
            if(s._edited && !s._toCreate){ EditTable(s); } 
            else if (s._toCreate){ CreateNewTable(s); }
            catalog_tbl.Advance();
      }
    } else return false;
}
bool Catalog::CreateNewTable(Schema& s){
    //write query to 
    return WriteSchema(s);
}
bool Catalog::EditTable(Schema& s){
    return WriteSchema(s);
}
bool Catalog::DeleteTables(){
    int total = tables_toDrop.Length(),
            dropped = 0;
    tables_toDrop.MoveToStart();
    while(!tables_toDrop.AtEnd()){
        string table_toDrop = tables_toDrop.CurrentData().operator string();
        if(DeleteTable(table_toDrop)){ dropped++;}
        tables_toDrop.Advance();
    }
    if(dropped == total){
        InefficientMap<Keyify<string>,Swapify<string> > empty;
        tables_toDrop.Swap(empty);
        return 1;}
    else return 0;
}
bool Catalog::DeleteTable(string& _tableName){
    //string query = "DROP TABLE " + _tableName + ";";
    //if(sqlite3_exec(_db,query.c_str(),0,0,0)== SQLITE_OK){
        string query_deleteFrom_Catalog = "DELETE FROM Catalog WHERE name = '" + _tableName + "'"; 
        if(sqlite3_exec(_db,query_deleteFrom_Catalog.c_str(),0,0,0)== SQLITE_OK){ 
            string query_deleteFrom_Attribute = "DELETE FROM Attribute WHERE tableName = '" + _tableName + "'"; 
            if(sqlite3_exec(_db,query_deleteFrom_Attribute.c_str(),0,0,0)== SQLITE_OK){ 
                return 1; 
            } else return 0;
        } else return 0; 
    //}
}
bool Catalog::WriteSchema(Schema& s){
    string query_Catalog = "INSERT OR REPLACE INTO Catalog(name,numTuples,location) VALUES(" //
            "'" + s._nameTable + "'," //
            "'" + extensions::to_string(s._noTuples) + "'," //
            "'" + s._location + "');";
    if(sqlite3_exec(_db,query_Catalog.c_str(),0,0,0) == SQLITE_OK){
        for(int i = 0; i < s.GetAtts().size(); i++){
            Attribute a = s.GetAtts().at(i);
            string query_Attribute = "INSERT OR REPLACE INTO Attribute(name,tableName,type,distinctTuple)  VALUES (" //
                    "'" + a.name + "'," //
                    "'" + s._nameTable + "'," //
                    "'" + ParseType(a.type) + "'," //
                    "'" + extensions::to_string(a.noDistinct) + "');";
            if(sqlite3_exec(_db,query_Attribute.c_str(),0,0,0) == SQLITE_OK){
                string table_name = s._nameTable;
                
                Keyify<string> key(table_name);
                vector<string> a;
                vector<string> b;
                vector<unsigned int> c;
                Schema empty(a,b,c);
                Swapify<Schema> ss(empty);
                catalog_tbl.Remove(key,key,ss);
                
                s._edited = false;
                s._toCreate = false;
                Swapify<Schema> sol(s);
                catalog_tbl.Insert(key,sol);
            }
            else return 0;
        }
    } else return 0;
    return 1;
}
string Catalog::ParseType(Type& type){
    switch(type){
        case Integer:{
            return "Integer";
        }
        case Float:{
            return "Float";
        }
        case String:{
            return "String";
        }
        default:{
            return "Unknown";
        }
    }
}
