#include <iostream>
#include <vector>
#include "stdio.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;


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
	
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	Keyify<string> key(_table); 
	CatalogEntry ce = catalog->Find(key);
	_path = ce.location;
	return true;
} 

void Catalog::SetDataFile(string& _table, string& _path) {
	
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
	
	
}

void Catalog::GetTables(vector<string>& _tables) {
	
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