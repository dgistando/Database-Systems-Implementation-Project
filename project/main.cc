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
        //menu goes here
        string tableName = "User";
        vector<string> attrb; attrb.push_back("Name"); attrb.push_back("Age");
        vector<string> atype; atype.push_back("VARCHAR"); atype.push_back("INTEGER");
        ctl.CreateTable(tableName, attrb, atype);
	return 0;
}
int printMenu (){

}
