#include <vector>
#include <string>
#include <iostream>
#include "stdio.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;


int main () {
	string db_name = "";
	printf("Enter the database: "); cin >> db_name;
	Catalog ctl = Catalog(db_name);
        if(!ctl.DatabaseOpen()){ printf("Database connection was not established. Closing..."); return 0; }
        if(!ctl.CatalogIsActive()){ printf("Unable to read catalog! Closing..."); return 0;}
        // create table User
        string tableName = "User";
        vector<string> attrb; attrb.push_back("Name"); attrb.push_back("Age");
        vector<string> atype; atype.push_back("VARCHAR"); atype.push_back("INTEGER");        
        ctl.CreateTable(tableName, attrb, atype);
        string tableName1 = "Customer";
        vector<string> attrb1; attrb1.push_back("Name"); attrb1.push_back("Gender");attrb1.push_back("Age");
        vector<string> atype1; atype1.push_back("VARCHAR"); atype1.push_back("VARCHAR"); atype1.push_back("INTEGER");        
        ctl.CreateTable(tableName1, attrb1, atype1);
        //get all tables
        vector<string> tbls;
        ctl.GetTables(tbls);
        for(int i = 0; i < tbls.size(); i++){ cout << tbls.at(i) << endl; }
        //get attributes
        vector<string> attr;
        ctl.GetAttributes(tableName,attr);
        for(int i = 0; i < attr.size(); i++){ cout << attr.at(i) << endl; }
        //get tuples
//        unsigned int test = 0;
//        ctl.GetNoTuples(tableName,test);
//        cout << "NoTuples read from catalog : " << test << endl;
//        //set tuples
//        test = 99;
//        ctl.SetNoTuples(tableName,test);
//        //get tupples
//        ctl.GetNoTuples(tableName,test);
//        cout << "NoTuples after SetTuples(" << test << ") : " << test << endl;
        //delete User table;
        ctl.DropTable(tableName);
//        vector<string> found_attrb;
//        ctl.GetAttributes(tableName,found_attrb);
//        for(int i = 0; i < found_attrb.size(); i++){
//            cout << found_attrb.at(i) << endl;
//        }
       
	return 0;
}
