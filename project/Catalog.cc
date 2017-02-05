#include <iostream>
#include "sqlite3.h"
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


Catalog::Catalog(string& _fileName) {
	sqlite3 *db;
  char *zErrMsg = 0;
  int  rc;
  char *sql;
  bool _isOpen;
  rc = sqlite3_open_v2(_fileName.c_str(), &_db, SQLITE_OPEN_READWRITE, 0);
  if(rc == SQLITE_OK){ _isOpen = 1; } else { _isOpen = 0; }
  /* Open database */



}
Catalog::~Catalog() {
	//sql close
	//save in database
}

bool Catalog::Save() {

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	return true;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {

}

bool Catalog::GetDataFile(string& _table, string& _path) {
	return true; //where is the fie that contains data
}

void Catalog::SetDataFile(string& _table, string& _path) {

}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	return true;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
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
