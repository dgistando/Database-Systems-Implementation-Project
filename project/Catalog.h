#ifndef _CATALOG_H
#define _CATALOG_H

#include <string>
#include <string.h>
#include <vector>
#include <iostream>

#include "sqlite3.h"
#include "Schema.h"

#include "TwoWayList.cc"
#include "Keyify.h"
#include "Swapify.cc"
#include "InefficientMap.cc"


using namespace std;


class Catalog {
private:
	/* Data structures to keep catalog data in memory.
	 * A series of data structures you may find useful are included.
	 * Efficient data structures are recommended.
	 * Avoid linear traversals when possible.
	 */
    class DBAccess{
    public:
        sqlite3 *_db;
        bool _isOpen;
        DBAccess(string &fileName){
            int rc = sqlite3_open_v2(fileName.c_str(), &_db, SQLITE_OPEN_READWRITE, 0);
            if(rc == SQLITE_OK){ _isOpen = 1; ReadCatalog(); } else { _isOpen = 0; }
        }
        ~DBAccess(){
            //write catalog
            WriteCatalog();
            //delete _db;
        }
        static int CatalogCallback(void *a_parameter, int argc, char **argv, char **colName){
//            char *columnName[] = { "name", "numTuples", "location" };
//            for (int i = 0; i < argc; i++){
//                char *tableName = new char;
//                char *location = new char;
//                int *nuTuples = new int;
//                for (int j = 0; j < sizeof(columnName)/sizeof(columnName[0]); j++){
//                    if (strcmp (colName[i], columnName[j]) == 0) {
//                    }
//                } 
//            }
            return 0;
        }
        static int AttributeCallback(void *a_parameter, int argc, char **argv, char **colName){
//            for (int i = 0; i < argc; i++){
//                
//            }
            return 0;
        }
        
        //int rc = sqlite3_exec(_db, sql.c_str(), CatalogCallback, 0, &err);
        bool ReadCatalog(){
            if(_isOpen){
                sqlite3_stmt *stmt; char *query = "SELECT * FROM Catalog";
                if(sqlite3_prepare(_db,query,-1,&stmt,0) == SQLITE_OK){
                    int ctotal = sqlite3_column_count(stmt);
                    int rc = 0;
                    while(1){
                        rc = sqlite3_step(stmt);
                        if(rc == SQLITE_ROW){
                            for(int i = 0; i < ctotal; i++){
                                string s = (char*)sqlite3_column_text(stmt,i);
                            }
                        }
                        if(rc != SQLITE_DONE){
                            
                        }
                    }
                }
            } else return 0;
        }
        bool WriteCatalog() {
            if(_isOpen){
                
            } else return 0;
        }
        
    };
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
    };
    class CatalogMap{
    private:
         InefficientMap<Keyify<string>,Swapify<CatalogEntry> > catalog_tbl;
         InefficientMap<Keyify<string>,Swapify<AttributeEntry> > attrb_tbl;
    public:
        CatalogMap(){ }
        ~CatalogMap(){ }
        bool InsertNewCatalogEntry(string &key_tableName, CatalogEntry &ce){
            Keyify<string> *key = new Keyify<string> (key_tableName); //ptr to keep it in mem
            if(!catalog_tbl.IsThere(*key)) {  Swapify<CatalogEntry> data (ce); catalog_tbl.Insert(*key,data); return 1; }
            else { return 0; } //throw("ERROR")?
        }
        bool InsertNewAttributeEntry(string &key_attrbName, AttributeEntry &ae){
            Keyify<string> *key = new Keyify<string> (key_attrbName); //ptr to keep it in mem
            if(!attrb_tbl.IsThere(*key)) {  Swapify<AttributeEntry> data (ae); attrb_tbl.Insert(*key,data); return 1; }
            else { return 0; } //throw("ERROR")?
        }
        bool GetCatalogEntry(string &key_tableName, CatalogEntry &ce){
            Keyify<string> key (key_tableName);
            if(catalog_tbl.IsThere(key)){ ce = catalog_tbl.Find(key); return 1; }
            else return 0;
        }
        bool GetAttributeEntry(string &key_attrbName, AttributeEntry &ae){
            Keyify<string> key (key_attrbName);
            if(attrb_tbl.IsThere(key)){ ae = attrb_tbl.Find(key); return 1; }
            else return 0;
        }
        void GetAllTables(vector<string> &tables){
            catalog_tbl.MoveToStart();
            while(!catalog_tbl.AtEnd()){
                tables.push_back(catalog_tbl.CurrentKey());
                catalog_tbl.Advance();
            }
        }
        void GetAllAttributes(string &tableName, vector<string> &attributes){
            /*Keyify<string> key(tableName);
            attrb_tbl.MoveToStart();
            while(!attrb_tbl.AtEnd()){
                if(attrb_tbl.CurrentKey().IsEqual(key)){
                    attributes.push_back(attrb_tbl.CurrentData().)
                }
            }*/
            //might need to reconsider the structure of the table
        }
    };
    DBAccess * _dbaccess;
    CatalogMap * _cmap;
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
