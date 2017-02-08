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
    
    string tableName1 = "Test";
    vector<string> attrb1; attrb1.push_back("a0"); attrb1.push_back("a2");
    vector<string> atype1; atype1.push_back("STRING"); atype1.push_back("INTEGER");
    ctl.CreateTable(tableName1,attrb1,atype1);
    
    
    
    string tableName = "User";
    unsigned int noTuples = 1;
    if(ctl.GetNoTuples(tableName,noTuples))
    { cout << "Before: " << extensions::to_string(noTuples) << endl; }
    noTuples = 101;
    ctl.SetNoTuples(tableName,noTuples);
    if(ctl.GetNoTuples(tableName,noTuples))
    { cout << "After: " << extensions::to_string(noTuples) << endl; }
    //test data
    
    
    string newLoc = "";
    if(ctl.GetDataFile(tableName,newLoc))
    { cout << "Before: " << newLoc << endl; }
    newLoc = "Mars";
    ctl.SetDataFile(tableName,newLoc);
    if(ctl.GetDataFile(tableName,newLoc))
    { cout << "After: " << newLoc << endl; }
    //tets no dist
    
    
    
    string attrbName = "id";
    unsigned int noDist= 0;
    if(ctl.GetNoDistinct(tableName,attrbName,noDist))
    { cout << "Before: " << extensions::to_string(noDist) << endl; }
    noDist= 777;
    ctl.SetNoDistinct(tableName,attrbName,noDist);
    if(ctl.GetNoDistinct(tableName,attrbName,noDist))
    { cout << "After: " << extensions::to_string(noDist) << endl; }
    //test get Tables
    
    vector<string> tbls;
    ctl.GetTables(tbls);
    for(int i = 0; i < tbls.size(); i++){ cout << "Table : " << tbls.at(i) << endl; }
    
    vector<string> attrb;
    if(ctl.GetAttributes(tableName,attrb)){
        for(int i = 0; i < attrb.size(); i++){ cout << "Attribute : " << attrb.at(i) << endl; } }
    
  
        vector<string> a;
        vector<string> b;
        vector<unsigned int> c;
    Schema s(a,b,c);
    ctl.GetSchema(tableName,s);
    cout << s << endl;
    
    cout << ctl;
    return 0;
}
