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
    if(ctl._dbOpen){
        
    }
    ctl.catalog_tbl.MoveToStart();
    ctl.catalog_tbl.Advance();
    cout << "THIS "  << ctl.catalog_tbl.CurrentData().operator Schema().GetAtts().size() << endl;
    string tableName = "User";
    unsigned int noTuples = 1;
    if(ctl.GetNoTuples(tableName,noTuples))
    { cout << "Before: " << extensions::to_string(noTuples) << endl; }
    noTuples = 101;
    ctl.SetNoTuples(tableName,noTuples);
    if(ctl.GetNoTuples(tableName,noTuples))
    { cout << "After: " << extensions::to_string(noTuples) << endl; }
    //test data
    
    ctl.catalog_tbl.MoveToStart();
    ctl.catalog_tbl.Advance();
    cout << "THIS "  << ctl.catalog_tbl.CurrentData().operator Schema().GetAtts().size() << endl;
    
    string newLoc = "";
    if(ctl.GetDataFile(tableName,newLoc))
    { cout << "Before: " << newLoc << endl; }
    newLoc = "Mars";
    ctl.SetDataFile(tableName,newLoc);
    if(ctl.GetDataFile(tableName,newLoc))
    { cout << "After: " << newLoc << endl; }
    //tets no dist
    
    ctl.catalog_tbl.MoveToStart();
    ctl.catalog_tbl.Advance();
    cout << "THIS "  << ctl.catalog_tbl.CurrentData().operator Schema().GetAtts().size() << endl;
    
    
    string attrbName = "id";
    unsigned int noDist= 0;
    if(ctl.GetNoDistinct(tableName,attrbName,noDist))
    { cout << "Before: " << extensions::to_string(noDist) << endl; }
    noDist= 777;
    ctl.SetNoDistinct(tableName,attrbName,noDist);
    if(ctl.GetNoDistinct(tableName,attrbName,noDist))
    { cout << "After: " << extensions::to_string(noDist) << endl; }
    return 0;
}
