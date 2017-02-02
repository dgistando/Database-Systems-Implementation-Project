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

	return 0;
}
int printMenu (){

}
