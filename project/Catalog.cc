#include <iostream>

#include "Schema.h"
#include "Catalog.h"



using namespace std;



Catalog::Catalog(string& _fileName) {
    _dbaccess = new DBAccess(_fileName);
    _cmap = new CatalogMap();
    
    
    InefficientMap<Keyify<string>,Swapify<CatalogEntry> > catalog_tbl;
    InefficientMap<Keyify<string>,Swapify<AttributeEntry> > attrb_tbl;
    if(_dbaccess->ReadCatalog(catalog_tbl,attrb_tbl) && 
            _cmap->CopyCatalog(catalog_tbl,attrb_tbl)){ _isCatalogActive = true; }
}

Catalog::~Catalog() {
    delete _dbaccess;
    delete _cmap;
    Save();
}

bool Catalog::Save() {
}
/*WORKS*/
bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
    CatalogEntry ce;
    if(_cmap->GetCatalogEntry(_table,ce)){ _noTuples = ce._noTuples; return true; }
    else return false;
}
/*WORKS*/
void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
        _cmap->SetNoTuples(_table,_noTuples);
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

/*WORKS*/
bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
    if(_cmap->TableExists(_table)){
        _cmap->GetTableAttributes(_table,_attributes);
        return true;
    }
    else return false;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	return true;
}

/*WORKS UNCOMMENT*/
bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
    if( _dbaccess->CreateTable(_table,_attributes,_attributeTypes) &&
        _cmap->CreateTable(_table,_attributes,_attributeTypes)){
        return true; }
    else return false;
}

bool Catalog::DropTable(string& _table) {
    
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}
