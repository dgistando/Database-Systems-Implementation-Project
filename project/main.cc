#include <vector>
#include <string>
#include <iostream>

#include "Schema.h"
#include "Catalog.h"

using namespace std;
namespace extensions
{
    template < typename T > string to_string( const T& n ){
        ostringstream stm; stm << n; return stm.str() ;
    }
}


int main () {
    string dbname = "";
    cout << "Enter Databse name:";
    cin >> dbname;
    Catalog ctl(dbname);
    string tableName = "User";
    unsigned int noTuples = 0;
    if(ctl.GetNoTuples(tableName,noTuples)){ cout << extensions::to_string(noTuples) << endl; }
    return 0;
}
