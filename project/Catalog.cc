#include <iostream>
#include <vector>
#include "stdio.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;

//Making sure its for my branch

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
  
	ptr = new InefficientMap<Keyify<int>, Swapify<int> >();
  
	if( sqlite3_open(_fileName.c_str(), &db) ){
		cout<<"Cant open database because: "<<sqlite3_errmsg(db)<<endl;
	}else{
		cout<<"Opened database successfully"<<endl;
	}
  
  //Also read all the data and store in structures
  
}

Catalog::~Catalog() {
	cout<<"Test for destructor"<<endl;
	//sql close
	//save in database
}

bool Catalog::Save() {
	cout<<"This is the save function"<<endl;

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	int rs, maxBytes = 64;
	sqlite3_stmt* statement;	
	string sql = "SELECT numTuples from Catalog WHERE name = '";
	sql.append(_table);
	sql.append("';");

	if(sqlite3_prepare(db, sql.c_str(), maxBytes, &statement, 0) == SQLITE_OK){
		if(sqlite3_step(statement) == SQLITE_ROW){
			_noTuples = atoi((char*)sqlite3_column_text(statement, 0));
			return true;
		}
	}
	
	return false;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	char* zErrMsg;
	int rs;
	string sql = "UPDATE Catalog SET numTuples = ";
	sql.append(_noTuples);
	sql.append(" WHERE name = '");
	sql.append(_table);
	sql.append("';");

	rs = sqlite3_exec(db, sql.c_str(), callback, 0, &zErrMsg);
	if( rs != SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	int rs, maxBytes = 64;
	sqlite3_stmt* statement;	
	string sql = "SELECT location from Catalog WHERE name = '";
	sql.append(_table);
	sql.append("';");

	if(sqlite3_prepare(db, sql.c_str(), maxBytes, &statement, 0) == SQLITE_OK){
		if(sqlite3_step(statement) == SQLITE_ROW){
			_path = (char*)sqlite3_column_text(statement, 0);
			return true;
		}
	}
	
	return false;
} 

void Catalog::SetDataFile(string& _table, string& _path) {
	char* zErrMsg;
	int rs;
	string sql = "UPDATE Catalog SET location = '";
	sql.append(_path);
	sql.append("' WHERE name = '");
	sql.append(_table);
	sql.append("';");

	rs = sqlite3_exec(db, sql.c_str(), callback, 0, &zErrMsg);
	if( rs != SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	int rs, maxBytes = 64;
	sqlite3_stmt* statement;	
	string sql = "SELECT distinctTuple from Attribute WHERE Name = '";
	sql.append(_attribute);
	sql.append("' AND table = '");
	sql.append(_table);
	sql.append("';");

	if(sqlite3_prepare(db, sql.c_str(), maxBytes, &statement, 0) == SQLITE_OK){
		if(sqlite3_step(statement) == SQLITE_ROW){
			_noDistinct = atoi((char*)sqlite3_column_text(statement, 0));
			return true;
		}
	}
	
	return false;
}

void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	char* zErrMsg;
	int rs;
	string sql = "UPDATE Attribute SET distinctTuple = ";
	sql.append(_noDistinct);
	sql.append(" WHERE Name = '");
	sql.append(_attribute);
	sql.append("' AND table = '");
	sql.append(_table);
	sql.append("';");

	rs = sqlite3_exec(db, sql.c_str(), callback, 0 , &zErrMsg);
	if(rs != SQLITE_OK){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
}

void Catalog::GetTables(vector<string>& _tables) {
	int rs, maxBytes = 64;
	sqlite3_stmt* statement; 
	string sql = "SELECT name FROM Catalog;";

	if(sqlite3_prepare(db,sql.c_str(), maxBytes, &statement, 0) == SQLITE_OK){
		rs = 0;
		while(rs != SQLITE_DONE){
			rs = sqlite3_step(statement);
			if(rs == SQLITE_ROW){
				_tables.push_back((char*)sqlite3_column_text(statement, 0));
			}
		}
	}
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	int rs, maxBytes = 64;
	sqlite3_stmt* statement;
	string sql = "SELECT Name from Attributes WHERE table = '";
	sql.append(_table);
	sql.append("';");

	if(sqlite3_prepare(db, sql, maxBytes, &statement, 0) == SQLITE_OK){
		rs = 0;
		while(rs != SQLITE_DONE){
			rs = sqlite3_step(statement);
			if(rs == SQLITE_ROW){
				_attributes((char*)sqlite3_column_text(statement, 0));	
			}
		}
		return true;
	}
	return false;	
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	int rs, maxBytes = 64;
	sqlite3_stmt* statement;
	string sql = "SELECT Name, type, distinctTuple WHERE table = '";
	sql.append(_table);
	sql.append("';");

	vector<string> Name;
	vector<string> type;
	vector<unsigned int> distinctTuple;
	
	if(sqlite3_prepare(db, sql, maxBytes, &statement, 0) == SQLITE_OK){
		rs = 0;
		while(rs != SQLITE_DONE){
			rs = sqlite3_step(statement);
			if(rs == SQLITE_ROW){
				Name.push_back((char*)sqlite3_column_text(statement, 0));
				type.push_back((char*)sqlite3_column_text(statement, 1));
				distinctTuple.push_back(atoi((char*)sqlite3_column_text(statement, 2)));
			}
		}
		_schema = new Schema(Name,type,distinctTuple);
		return true;
	}
	return false;	
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
	
	char *zErrMsg = 0;
	int  rc;
	string sql = "CREATE TABLE ";
	sql.append(_table);
	sql.append("(");

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
