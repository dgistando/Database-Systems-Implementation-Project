#include <iostream>

#include "Schema.h"
#include "Catalog.h"



using namespace std;



Catalog::Catalog(string& _fileName) {
    _dbaccess = new DBAccess(_fileName);
    _cmap = new CatalogMap();
    
    
    InefficientMap<Keyify<string>,Swapify<CatalogEntry> > catalog_tbl;
    InefficientMap<Keyify<string>,Swapify<TableSchema> > attrb_tbl;
    if(_dbaccess->ReadCatalog(catalog_tbl,attrb_tbl) && 
            _cmap->CopyCatalog(catalog_tbl,attrb_tbl)){ _isCatalogActive = true; }
}

Catalog::~Catalog() {
    Save();
    delete _dbaccess;
    delete _cmap;
}

bool Catalog::Save() {
    if(_dbaccess->WriteCatalog(_cmap->GetCatalogMapObject(),
            _cmap->GetAttributeMapObject())){ return true; } 
    else return false;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
    CatalogEntry ce;
    if(_cmap->GetCatalogEntry(_table,ce)){ _noTuples = ce._noTuples; return true; }
    else return false;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
        _cmap->SetNoTuples(_table,_noTuples);
}

bool Catalog::GetDataFile(string& _table, string& _path) {
    CatalogEntry ce;
    if(_cmap->GetCatalogEntry(_table,ce)){ _path = ce._location; return true; }
    else return false;
}

void Catalog::SetDataFile(string& _table, string& _path) {
    _cmap->SetLocationFile(_table,_path);
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
    AttributeEntry ae;
    if(_cmap->GetAttributeEntry(_table,_attribute,ae)){ _noDistinct = ae._noDistinct; return true; }
    else return false;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
    _cmap->SetNoDistinct(_table,_attribute,_noDistinct);
}

void Catalog::GetTables(vector<string>& _tables) {
    _cmap->GetAllTables(_tables);
}
/*not working check the CatalogMap method*/
bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
    if(_cmap->GetTableAttributes(_table,_attributes)){ return true; }
    else return false;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
    return true;
}
/* Rewrite the if(db.create && cmap.create)
 * In such a way that each task is done separately
 * In order to have safe fallback on catalog in memory
 */
bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
    if(!_cmap->TableExists(_table)){
        if( _dbaccess->CreateTable(_table,_attributes,_attributeTypes) &&
            _cmap->CreateTable(_table,_attributes,_attributeTypes)){
            return true; }
        else return false;
    } else return false;
}
/* Rewrite the if(db.drop && cmap.drop)
 * In such a way that each task is done separately
 * In order to have safe fallback on catalog in memory
 */
bool Catalog::DropTable(string& _table) {
    if(_cmap->TableExists(_table)){
        if(_dbaccess->DropTable(_table) && 
            _cmap->DropTable(_table)){
            return true; }
        else return false;
    } else false;
    return true;
}


ostream& operator<<(ostream& _os, Catalog& _c) {
    //_os << _c._cmap->PrintCatalog();
    return _os;
}
