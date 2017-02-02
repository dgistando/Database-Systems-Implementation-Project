#include <iostream>

#include "Schema.h"
#include "Catalog.h"



using namespace std;



Catalog::Catalog(string& _fileName) {
    _dbaccess = new DBAccess(_fileName);
    _cmap = new CatalogMap();
}

Catalog::~Catalog() {
    delete _dbaccess;
    delete _cmap;
    Save();
}

bool Catalog::Save() {

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
    CatalogEntry *ce;
    if(_cmap->GetCatalogEntry(_table,*ce)){ _noTuples = ce->_noTuples; return 1; }
    else return 0;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
    CatalogEntry *ce;
    if(_cmap->GetCatalogEntry(_table, *ce)){ ce->_noTuples = _noTuples; }
    else { /*throw("MADNESS");*/ } 
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
    _cmap->GetAllTables(_tables);
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
