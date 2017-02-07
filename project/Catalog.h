#ifndef _CATALOG_H
#define _CATALOG_H

#include <string>
#include <sstream> // needed to convcert stupid int to string ffs
#include <set> // lazy sorting
#include <vector>
#include <iostream>

#include "sqlite3.h"
#include "Schema.h"

#include "TwoWayList.cc"
#include "Keyify.h"
#include "Swapify.cc"
#include "InefficientMap.cc"


using namespace std;

namespace extensions
{
    template < typename T > string to_string( const T& n ){
        ostringstream stm; stm << n; return stm.str() ;
    }
}


class Catalog {
private:
	/* Data structures to keep catalog data in memory.
	 * A series of data structures you may find useful are included.
	 * Efficient data structures are recommended.
	 * Avoid linear traversals when possible.
	 */
    class CatalogEntry{
    public:
        string _tableName;
        int _noTuples;
        string _location;
        CatalogEntry(){}
        CatalogEntry(string tableName, int noTuples, string location){
            _tableName = tableName; _noTuples = noTuples; _location = location;
        }
        ~CatalogEntry(){}
        
    };
    class AttributeEntry{
    public:
        string _attrbName;
        string _tableName;
        string _type;
        int _noDistinct;
        AttributeEntry(){}
        AttributeEntry(string attrbname, string tableName, string type, int noDistinct){
            _attrbName = attrbname; _tableName = tableName; _type = type; _noDistinct = noDistinct; 
        }
        ~AttributeEntry(){}
        bool isFromTable(string &_table){
            if(_tableName == _table) { return 1; }
            else return 0;
        }
    };
    class DBAccess{
    public:
        sqlite3 *_db;
        bool _isOpen;
        DBAccess(string &fileName){
            int rc = sqlite3_open_v2(fileName.c_str(), &_db, SQLITE_OPEN_READWRITE, 0);
            if(rc == SQLITE_OK){ _isOpen = 1; } else { _isOpen = 0; }
        }
        ~DBAccess(){
            //delete _db;
        }
        
        
        bool ReadCatalog(InefficientMap<Keyify<string>,Swapify<CatalogEntry> > &catalog_tbl,InefficientMap<Keyify<string>,Swapify<AttributeEntry> > &attrb_tbl){
            if(_isOpen){
                if(ReadTable_Catalog(catalog_tbl) && ReadTable_Attribute(attrb_tbl)){ return 1; }
                else return 0;
            } else return 0;
        }
        bool ReadTable_Catalog(InefficientMap<Keyify<string>,Swapify<CatalogEntry> > &catalog_tbl){
            try{
                sqlite3_stmt *stmt; string query = "SELECT * FROM Catalog";
                if(sqlite3_prepare(_db,query.c_str(),-1,&stmt,0) == SQLITE_OK){
                    int ctotal = sqlite3_column_count(stmt);
                    int rc = 0;
                    while(1){   //goes one row at the time
                        rc = sqlite3_step(stmt);
                        if(rc == SQLITE_ROW){
                            
                            string tableName = "";
                            int noTuples = 0;
                            string location = "";
                            
                            for(int i = 0; i < ctotal; i++){
                                switch(i){
                                    case 0:{ // catalog.tableName
                                        tableName = (char*)sqlite3_column_text(stmt,i);
                                        break;
                                    }
                                    case 1:{ // catalog.numTuples
                                        noTuples = sqlite3_column_int(stmt,i);
                                        break;
                                    }
                                    case 2:{ // catalog.location
                                        location = (char*)sqlite3_column_text(stmt,i);
                                        break;
                                    }
                                };
                            }
                            Keyify<string> key(tableName);
                            CatalogEntry ce(tableName,noTuples,location);
                            Swapify<CatalogEntry> sce(ce);
                            catalog_tbl.Insert(key,sce);
                        }
                        if(rc == SQLITE_ERROR){ return 0; }
                        if(rc == SQLITE_DONE){ break; }
                    }
                }
                sqlite3_finalize(stmt);
            }catch (const char* msg){ return 0; }
            return 1;
        }
        bool ReadTable_Attribute(InefficientMap<Keyify<string>,Swapify<AttributeEntry> > &attrb_tbl){
            try{
                
                sqlite3_stmt *stmt; string query = "SELECT * FROM Attribute";
                if(sqlite3_prepare(_db,query.c_str(),-1,&stmt,0) == SQLITE_OK){
                    int ctotal = sqlite3_column_count(stmt);
                    int rc = 0;
                    while(1){   //goes one row at the time
                        rc = sqlite3_step(stmt);
                        if(rc == SQLITE_ROW){
                            
                            string name = "";
                            string tableName = "";
                            string type = "";
                            int distinctTuple = 0;
                            for(int i = 0; i < ctotal; i++){
                                switch(i){
                                    case 0:{ // attribute.name
                                        name = (char*)sqlite3_column_text(stmt,i);
                                        break;
                                    }
                                    case 1:{ // attribute.tableName
                                        tableName = (char*)sqlite3_column_text(stmt,i);
                                        break;
                                    }
                                    case 2:{ // attribute.type
                                        type = (char*)sqlite3_column_text(stmt,i);
                                        break;
                                    }
                                    case 3:{ // attribute.distinctTuple
                                        distinctTuple = sqlite3_column_int(stmt,i);
                                        break;
                                    }
                                };
                            }
                            string key_str = tableName + "_" + name;
                            Keyify<string> key(key_str);
                            AttributeEntry ce(name,tableName,type,distinctTuple);
                            Swapify<AttributeEntry> sce(ce);
                            attrb_tbl.Insert(key,sce);
                        }
                        if(rc == SQLITE_ERROR){ return 0; }
                        if(rc == SQLITE_DONE){ break; }
                    }
                }
                sqlite3_finalize(stmt);
            } catch(const char* msg) { return 0; }
            return 1;
        }
        
        bool WriteCatalog(InefficientMap<Keyify<string>,Swapify<CatalogEntry> > &catalog_tbl,InefficientMap<Keyify<string>,Swapify<AttributeEntry> > &attrb_tbl) {
            if(_isOpen){
                if(Write_Catalog(catalog_tbl) && Write_Attribute(attrb_tbl)){ return 1; }
                else return 0;
            } else return 0;
        }
        bool Write_Catalog(InefficientMap<Keyify<string>,Swapify<CatalogEntry> > &catalog_tbl){
            int total = catalog_tbl.Length(), updated = 0;
            try{
                catalog_tbl.MoveToStart();
                while(!catalog_tbl.AtEnd()){
                    CatalogEntry ce = catalog_tbl.CurrentData().operator CatalogEntry();
                    sqlite3_stmt *stmt;
                    string query = "INSERT OR REPLACE INTO Catalog(name,numTuples,location)  VALUES (" //
                            "'" + ce._tableName + "'," //
                            "'" + extensions::to_string(ce._noTuples) + "'," //
                            "'" + ce._location + "')";
                    if(sqlite3_prepare(_db,query.c_str(),-1,&stmt,0) == SQLITE_OK){
                        int rc = sqlite3_step(stmt);
                        if(rc == SQLITE_DONE){ updated++;}
                    }
                    sqlite3_finalize(stmt);
                    catalog_tbl.Advance();
                }
            } catch(const char* msg){ return 0; }
            if(total == updated) return 1;
            else return 0;
        }
        bool Write_Attribute(InefficientMap<Keyify<string>,Swapify<AttributeEntry> > &attrb_tbl){
           int total = attrb_tbl.Length(), updated = 0;
            try{
                attrb_tbl.MoveToStart();
                while(!attrb_tbl.AtEnd()){
                    AttributeEntry ae = attrb_tbl.CurrentData().operator AttributeEntry();
                    sqlite3_stmt *stmt;
                    string query = "INSERT OR REPLACE INTO Attribute(name,tableName,type,distinctTuple)  VALUES (" //
                            "'" + ae._attrbName + "'," //
                            "'" + ae._tableName + "'," //
                            "'" + ae._type + "'," //
                            "'" + extensions::to_string(ae._noDistinct) + "')";
                    if(sqlite3_prepare(_db,query.c_str(),-1,&stmt,0) == SQLITE_OK){
                        int rc = sqlite3_step(stmt);
                        if(rc == SQLITE_DONE){ updated++;}
                    }
                    sqlite3_finalize(stmt);
                    attrb_tbl.Advance();
                }
            } catch(const char* msg){ return 0; }
            if(total == updated) return 1;
            else return 0; 
        }
        
        bool CreateTable(string& _table, vector<string>& _attributes,vector<string>& _attributeTypes){
            /* TO DO:
             * Create something to check if the table was created* like bool
             * Create something to check if the table was registered in Catalog& and Attribute* like bool
             * After, check if either failed
             * If table was created but not registered, pass true and write in the catalog when program is finished
             * else ..?
             */
            if(_isOpen){
                string query = "CREATE TABLE " + _table + "(";
                for(int i = 0; i < _attributes.size(); i++){
                    if(i != _attributes.size() - 1) { query += _attributes.at(i) + " " + _attributeTypes.at(i) + ","; }
                    else { query += _attributes.at(i) + " " + _attributeTypes.at(i); }
                } query += ");";
                if(sqlite3_exec(_db,query.c_str(),0,0,0)== SQLITE_OK)
                {
                    query = "INSERT INTO Catalog(name, numTuples,location) VALUES('" + _table + "',0,'test')";
                    if(sqlite3_exec(_db,query.c_str(),0,0,0)== SQLITE_OK)
                    {
                        for(int i = 0; i < _attributes.size(); i++){
                            query = "INSERT INTO Attribute(name,tableName,type,distinctTuple) VALUES('" + _attributes.at(i) + "','" + _table + "','" + _attributeTypes.at(i) + "',0)";
                            if(sqlite3_exec(_db,query.c_str(),0,0,0) == SQLITE_OK)
                            {
                                //?
                            } else return 0;
                        } return 1;
                    } else return 0;
                } else return 0;
            } else return 0;
        }
        bool DropTable(string& _table){
            if(_isOpen){
                
            }
            return 0;
        }
    };
    class CatalogMap{
    private:
         InefficientMap<Keyify<string>,Swapify<CatalogEntry> > *catalog_tbl;
         InefficientMap<Keyify<string>,Swapify<AttributeEntry> > *attrb_tbl;
    public:
        CatalogMap(){
            catalog_tbl = new InefficientMap<Keyify<string>,Swapify<CatalogEntry> > ();
            attrb_tbl = new InefficientMap<Keyify<string>,Swapify<AttributeEntry> > ();
        }
        ~CatalogMap(){
            delete catalog_tbl;
            delete attrb_tbl;
        }
        
        InefficientMap<Keyify<string>,Swapify<CatalogEntry> > & GetCatalogMapObject(){ return *catalog_tbl; }
        InefficientMap<Keyify<string>,Swapify<AttributeEntry> > & GetAttributeMapObject() { return *attrb_tbl; }
        
        
        /* If table exists, gets the CatalogEntry and return true
         * Otherwise return false
         */
        bool GetCatalogEntry(string &key_tableName, CatalogEntry &ce){
            Keyify<string> key (key_tableName);
            if(catalog_tbl->IsThere(key)){ 
                ce = catalog_tbl->Find(key).operator CatalogEntry();
                return 1; 
            } else return 0;
        }
        bool GetAttributeEntry(string &tableName, string &attrbName, AttributeEntry &ae){
            string key_str = tableName + "_" + attrbName;
            Keyify<string> key (key_str);
            if(attrb_tbl->IsThere(key)){ 
                ae = attrb_tbl->Find(key).operator AttributeEntry();
                return 1; 
            } else return 0;
        }
        
        /*  Linearly iterates through the catalog
         *  Pushes map keys (table name) into the vector to return
         */
        void GetAllTables(vector<string> &tables){
//            catalog_tbl.MoveToStart();
//            while(!catalog_tbl.AtEnd()){
//                tables.push_back(catalog_tbl.CurrentKey());
//                catalog_tbl.Advance();
//            }
        }
        void GetTableAttributes(string &tableName, vector<string> &attributes){
            
//            Keyify<string> key(tableName);
//            attrb_tbl->MoveToStart();
//            while(!attrb_tbl->AtEnd()){
//                if(attrb_tbl->CurrentData().operator AttributeEntry().isFromTable(tableName)){
//                    attributes.push_back(attrb_tbl->CurrentKey().operator string());
//                }
//                attrb_tbl->Advance();
//            }
        }

        void SetLocationFile(string &_table, string &_path){
            CatalogEntry ce;
            if(GetCatalogEntry(_table,ce)){
                Keyify<string> key(_table);
                Swapify<CatalogEntry> sce (ce);
                catalog_tbl->Remove(key,key,sce);
                ce._location = _path;
                sce = Swapify<CatalogEntry> (ce);
                catalog_tbl->Insert(key,sce);
            } else { /*throw("Madness")*/ }
        }
        void SetNoTuples(string& _table, unsigned int& _noTuples){
            CatalogEntry ce;
            if(GetCatalogEntry(_table,ce)){
                Keyify<string> key(_table);
                Swapify<CatalogEntry> sce (ce);
                catalog_tbl->Remove(key,key,sce);
                ce._noTuples = _noTuples;
                sce = Swapify<CatalogEntry> (ce);
                catalog_tbl->Insert(key,sce);
            } else { /*throw("madness");*/}
        }
        void SetNoDistinct(string& _table, string& _attribute,
            unsigned int& _noDistinct){
            AttributeEntry ae;
            if(GetAttributeEntry(_table,_attribute,ae)){
                string key_str = _table + "_" + _attribute;
                Keyify<string> key(key_str);
                Swapify<AttributeEntry> sae(ae);
                attrb_tbl->Remove(key,key,sae);
                ae._noDistinct = _noDistinct;
                sae = Swapify<AttributeEntry>(ae);
                attrb_tbl->Insert(key,sae);
            } else { /*throw("madness");*/}
        }
        
        bool CreateTable(string& _table, vector<string>& _attributes, vector<string>& _attributeTypes){
            try{
                CatalogEntry ce(_table,0,"catalog");
                Swapify<CatalogEntry> sce(ce);
                Keyify<string> key(_table);
                catalog_tbl->Insert(key,sce);
                for(int i = 0; i < _attributes.size(); i++){
                    AttributeEntry ae (_attributes.at(i),_table,_attributeTypes.at(i),0);
                    Swapify<AttributeEntry> sae (ae);
                    Keyify<string> key_a (_attributes.at(i));
                    attrb_tbl->Insert(key_a,sae);
                }
                return 1;
            } catch(const char* msg) { return 0; }
        }
        bool DropTable(string& _table){
            
        }
        
        bool TableExists(string &_table){
            Keyify<string> key (_table);
            if(catalog_tbl->IsThere(key)){ return 1; }
            else return 0;
        }
        bool CopyCatalog(InefficientMap<Keyify<string>,Swapify<CatalogEntry> > &_catalog_tbl,InefficientMap<Keyify<string>,Swapify<AttributeEntry> > &_attrb_tbl){
            int c_size = _catalog_tbl.Length();
            int a_size = _attrb_tbl.Length();
            catalog_tbl->Swap(_catalog_tbl);
            attrb_tbl->Swap(_attrb_tbl);
            if(catalog_tbl->Length() == c_size && attrb_tbl->Length() == a_size){ return 1; }
            else return 0;
        }
    
        void TestPrint(){
            catalog_tbl->MoveToStart();
            int i = catalog_tbl->Length();
            while(!catalog_tbl->AtEnd()){
                CatalogEntry ce = catalog_tbl->CurrentData().operator CatalogEntry();
                cout << "Table: " << ce._tableName << " noTuples : " << ce._noTuples << " Location: " << ce._location << endl;
                catalog_tbl->Advance();
            }
        }
        /*broken*/
        string PrintCatalog(){
//            string retval = "";
//            catalog_tbl->MoveToStart();
//            while(!catalog_tbl->AtEnd()){
//                Swapify<CatalogEntry> entry = catalog_tbl->CurrentData();
//                CatalogEntry op = entry.operator CatalogEntry();
//                vector<string> attrbutes;
//                vector<AttributeEntry> attrb_obj;
//                GetTableAttributes(op._tableName,attrbutes);
//                for(int i = 0; i < attrbutes.size(); i++){
//                    AttributeEntry ae;
//                    GetAttributeEntry(attrbutes.at(i),ae);
//                    attrb_obj.push_back(ae);
//                }
//                retval = op._tableName + "\t" + extensions::to_string(op._noTuples) + "\t" + op._location + "\n";
//                set<string> bin;
//                for(int i = 0; i < attrb_obj.size(); i++){
//                    AttributeEntry ae = attrb_obj.at(i);
//                    string a_str = "\t" + ae._attrbName + "\t" + ae._type + "\t" + extensions::to_string(ae._noDistinct) + "\n";
//                    bin.insert(a_str);
//                }
//                set<string>::iterator iter = bin.begin();
//                while(iter != bin.end()){
//                    retval += *iter;
//                    iter++;
//                }
//                retval += "\n\n";
//                catalog_tbl->Advance();
//            }
//            return retval;
        }
    };
    
    DBAccess * _dbaccess;
    CatalogMap * _cmap;
    bool _isCatalogActive;
    
public:
	/* Catalog constructor.
	 * Initialize the catalog with the persistent data stored in _fileName.
	 * _fileName is a SQLite database containing data on tables and their attributes.
	 * _fileName is queried through SQL.
	 * Populate in-memory data structures with data from the SQLite database.
	 * All the functions work with the in-memory data structures.
	 */
	Catalog(string& _fileName);

	/* Catalog destructor.
	 * Store all the catalog data in the SQLite database.
	 */
	virtual ~Catalog();

	/* Save the content of the in-memory catalog to the database.
	 * Return true on success, false otherwise.
	 */
	bool Save();

	/* Get/Set the number of tuples in _table.
	 * Get returns true if _table exists, false otherwise.
	 */
	bool GetNoTuples(string& _table, unsigned int& _noTuples);
	void SetNoTuples(string& _table, unsigned int& _noTuples);

	/* Get/Set the location of the physical file containing the data.
	 * Get returns true if _table exists, false otherwise.
	 */
	bool GetDataFile(string& _table, string& _path);
	void SetDataFile(string& _table, string& _path);

	/* Get/Set the number of distinct elements in _attribute of _table.
	 * Get returns true if _table exists, false otherwise.
	 */
	bool GetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct);
	void SetNoDistinct(string& _table, string& _attribute, unsigned int& _noDistinct);

	/* Return the tables from the catalog.
	 */
	void GetTables(vector<string>& _tables);

	/* Return the attributes of _table in _attributes.
	 * Return true if _table exists, false otherwise.
	 */
	bool GetAttributes(string& _table, vector<string>& _attributes);

	/* Return the schema of _table in _schema.
	 * Return true if _table exists, false otherwise.
	 */
	bool GetSchema(string& _table, Schema& _schema);

	/* Add a new table to the catalog with the corresponding attributes and types.
	 * The only possible types for an attribute are: INTEGER, FLOAT, and STRING.
	 * Return true if operation successful, false otherwise.
	 * There can be a single table with a given name in the catalog.
	 * There can be a single attribute with a given name in a table.
	 */
	bool CreateTable(string& _table, vector<string>& _attributes,
		vector<string>& _attributeTypes);

	/* Delete table from the catalog.
	 * Return true if operation successful, i.e., _table exists, false otherwise.
	 */
	bool DropTable(string& _table);
        
        /* Checks if the Database is open
         * Returns true if there exists Database connection
         * Returns false if the _dbaccess pointer is NULL or 
         * there is no Database connection
         */
        bool DatabaseOpen() { if(_dbaccess) { return _dbaccess->_isOpen; } else return 0; }
        bool CatalogIsActive() { if (_isCatalogActive){ return 1; } else return 0; }

	/* Overload printing operator for Catalog.
	 * Print the content of the catalog in a friendly format:
	 * table_1 \tab noTuples \tab pathToFile
	 * \tab attribute_1 \tab type \tab noDistinct
	 * \tab attribute_2 \tab type \tab noDistinct
	 * ...
	 * table_2 \tab noTuples \tab pathToFile
	 * \tab attribute_1 \tab type \tab noDistinct
	 * \tab attribute_2 \tab type \tab noDistinct
	 * ...
	 * Tables/attributes are sorted in ascending alphabetical order.
	 */
	friend ostream& operator<<(ostream& _os, Catalog& _c);
};

#endif //_CATALOG_H
