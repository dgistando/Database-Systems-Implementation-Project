#include <iostream>
<<<<<<< HEAD
=======
#include <vector>
#include "stdio.h"
>>>>>>> c04b4f53c8cd227b4f2bb66b5bf33e560e308393

#include "Schema.h"
#include "Catalog.h"

using namespace std;
namespace extensions
{
    template < typename T > string to_string( const T& n ){
        ostringstream stm; stm << n; return stm.str() ;
    }
}


<<<<<<< HEAD
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
=======
static int callback(void *NotUsed, int argc, char **argv, char **azColName){
   int i;
   for(i = 0; i < argc; i++){
      printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
   }
   printf("\n");
   return 0;
}

Catalog::Catalog(string& _fileName) {cout<<"Catalog constructor"<<endl;
  /* Open database */
  
	catalog = new InefficientMap<Keyify<string>, Swapify<CatalogEntry> >();
	attributes = new InefficientMap<Keyify<string>, Swapify<Schema> >();
	
	Keyify<string> KeyEntry;
	Swapify<CatalogEntry> ValueEntry;
	
	if( sqlite3_open(_fileName.c_str(), &db) ){
		cout<<"Cant open database because: "<<sqlite3_errmsg(db)<<endl;
	}else{
		cout<<"Opened database successfully"<<endl;
	}

  //Also read all the data and store in structures
    int rs, maxBytes = 64;
	sqlite3_stmt* statement; 
	string sql = "SELECT * FROM Catalog;";

	if(sqlite3_prepare(db,sql.c_str(), maxBytes, &statement, 0) == SQLITE_OK){
		rs = 0;
		while(rs != SQLITE_DONE){
			rs = sqlite3_step(statement);
			if(rs == SQLITE_ROW){
								
				KeyEntry = Keyify<string>((char*)sqlite3_column_text(statement,0));
				ValueEntry = Swapify<CatalogEntry>(CatalogEntry(
					//name
					(char*)sqlite3_column_text(statement,0),
					//numTuple
					atoi((char*)sqlite3_column_text(statement,1)),
					//location
					(char*)sqlite3_column_text(statement,2) 
				));
	 
				//cout<<"somevlaue"<<endl;
				catalog->Insert(KeyEntry, ValueEntry);
				cout<<catalog->Length()<<endl;
			}
		}
	}
	
	if(sqlite3_finalize(statement) != SQLITE_OK){return;}
	Swapify<Schema>* SValueEntry;
	
	sql = "SELECT * FROM Attribute;";

	vector<string>* _attributes = new vector<string>();
	vector<string>* _attributeTypes = new vector<string>();
	vector<unsigned int>* _distincts = new vector<unsigned int>;
	
	if(sqlite3_prepare(db,sql.c_str(), maxBytes, &statement, 0) == SQLITE_OK){
		rs = 0;
		string tempTable = "null";
		
		while(rs != SQLITE_DONE){
			rs = sqlite3_step(statement);
			if(rs == SQLITE_ROW){
				//Only for new table seen after first run
				if(tempTable != "null" && tempTable != (char*)sqlite3_column_text(statement,1)){
					KeyEntry = Keyify<string>(tempTable);
					Schema* schema = new Schema(*_attributes, *_attributeTypes, *_distincts);
					SValueEntry = new Swapify<Schema>(Schema(*schema));
					
					//add to structure
					attributes->Insert(KeyEntry, *SValueEntry);
					//reset vector data
					_attributes = new vector<string>();
					_attributeTypes = new vector<string>();
					_distincts = new vector<unsigned int>();	

					tempTable = (char*)sqlite3_column_text(statement,1);
					
					//This is the initial case
				}else if(tempTable == "null"){
					tempTable = (char*)sqlite3_column_text(statement, 1);
				}
				
				/*cout<<tempTable<<endl;
				cout<<"pushing back: "<<(char*)sqlite3_column_text(statement,0)<<endl;				
				cout<<"pushing back: "<<(char*)sqlite3_column_text(statement,2)<<endl;
				cout<<"pushing back: "<<(char*)sqlite3_column_text(statement,3)<<endl;*/
				
				//add to vectors here
				_attributes->push_back((char*)sqlite3_column_text(statement,0));
				_attributeTypes->push_back((char*)sqlite3_column_text(statement,2));
				_distincts->push_back(atoi((char*)sqlite3_column_text(statement,3)));
				
				//Load final data after last row
			}else if(rs == SQLITE_DONE){
				KeyEntry = Keyify<string>(tempTable);
				Schema* schema = new Schema(*_attributes, *_attributeTypes, *_distincts);
				SValueEntry = new Swapify<Schema>(Schema(*schema));
				//add to structure
				attributes->Insert(KeyEntry, *SValueEntry);
			}

		}
	}
	

	//Example Retreival
	/*cout<<"TEST"<<endl;
	//I know this isnt the cleanest way to do it//
	Keyify<string> key("table1"); 
	CatalogEntry ce = catalog->Find(key);
	cout<<ce.location<<endl;	
	
	Keyify<string> keys("table1"); 
	Schema ae = attributes->Find(keys);
	cout<<ae<<endl;*/
  
  
}

Catalog::~Catalog() {
	cout<<"Test for destructor"<<endl;
	//sql close
	sqlite3_close(db);
	//save in database
}

bool Catalog::Save() {
	cout<<"This is the save function"<<endl;

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	Keyify<string> key(_table); 
	CatalogEntry ce = catalog->Find(key);
	_noTuples = ce.noTuples;
	return true;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	//check map for key
	string location; GetDataFile(_table, location);
	Keyify<string> key(_table);
	Keyify<string> newKey(_table);
	Swapify<CatalogEntry> ValueEntry(CatalogEntry(
					//name
					_table,
					//numTuple
					_noTuples,
					//location
					location
				));
	
	//Not sure if works. Maybe delete and insert if not.
	if(catalog->IsThere(key)){
		if(catalog->Remove(key, newKey, ValueEntry)){
			catalog->Insert(newKey, ValueEntry);
		}
	}
	
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	Keyify<string> key(_table); 
	CatalogEntry ce = catalog->Find(key);
	_path = ce.location;
	return true;
} 

void Catalog::SetDataFile(string& _table, string& _path) {
	//check map for key
	/*Keyify<string> key(_table); 
	CatalogEntry ce;
	//Not sure if works. Maybe delete and insert if not.(Just to be sure)
	if(catalog->IsThere(key)){
		ce = catalog->Find(key);
		ce.setLocation(_path);
	}*/
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	Keyify<string> keys(_table); 
	Schema sc = attributes->Find(keys);
	_noDistinct = sc.GetDistincts(_attribute);
	if(_noDistinct == -1){ 
		return false;
	}else{
		return true;
	}
}

void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	/*Keyify<string> keys(_table); 
	Schema sc = attributes->Find(keys);
	//STOPPED HERE!!!
	//
	//*/
}

void Catalog::GetTables(vector<string>& _tables) {
	catalog->MoveToStart();
	if(catalog->AtStart()){
		while(!catalog->AtEnd()){
			CatalogEntry ce = catalog->CurrentData();
			_tables.push_back(ce.tableName);
			catalog->Advance();
		}
	}
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	Keyify<string> keys(_table); 
	Schema sc = attributes->Find(keys);
	vector<Attribute> atts = sc.GetAtts();
	for(int i=0; i<atts.size(); i++){
		_attributes.push_back(atts.at(i).name);
	}
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	Keyify<string> keys(_table); 
	Schema sc = attributes->Find(keys);
	_schema = sc;
	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
	
	char *zErrMsg = 0;
	int  rc;
	string sql = "CREATE TABLE ";
	sql.append(_table);
	sql.append(" (");

	//Databse should be open	

	/* Create SQL statement */
	if(_attributes.size() == _attributeTypes.size()){       
		for(int i = 0; i < _attributes.size(); i++){
				//append the values here
				sql.append(_attributes[i]);
				sql.append(" ");
				sql.append(_attributeTypes[i]);
			if(i != _attributes.size() - 1)sql.append(",");
        }       
    }else{return false;}

        sql.append(");");

	/* Execute SQL statement */
	rc = sqlite3_exec(db, sql.c_str(), callback, 0, &zErrMsg);
	if( rc != SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
		return false;
	}else{
		fprintf(stdout, "Table created successfully\n");
		return true;
	}	
}

bool Catalog::DropTable(string& _table) {
	char* zErrMsg;
	int rs;
	string sql = "DELETE FROM Catalog WHERE name = '";
	sql.append(_table);
	sql.append("';");

	rs = sqlite3_exec(db, sql.c_str(), callback, 0 , &zErrMsg);
	if(rs != SQLITE_OK){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
		return false;
	}
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	//os << _c
	return _os;
}
>>>>>>> c04b4f53c8cd227b4f2bb66b5bf33e560e308393
