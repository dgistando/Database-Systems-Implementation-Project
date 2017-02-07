#include <vector>
#include <string>
#include <iostream>

#include "Schema.h"
#include "Catalog.h"

using namespace std;


bool getInput(string table){
	
}

int main () {
	string db_name = "";
	cout << "Enter the database: ";
	cin >> db_name;
	if(db_name != ""){
		Catalog* catalog = new Catalog(db_name);
	}
	char in = 0;
	string table = "";
	
	cout<<"\n\tMENU"<<endl;
	cout<<"1)CREATE TABLE\n2)DROP TABLE\n3)DISPAY CATALOG\n\n q: quit"<<endl;
	cin>>in;
	while(1){
		switch(in){
			case'1':
				cout<<"CREATE TABLE"<<endl;
				cin>>table;
				cout<<table<<endl;
				//if(!getInput(table))cout<<"Table creation cancelled"<<endl;
				break;		
			case'2':
				cout<<"DROP TABLE"<<endl;
				cin>>table;
				cout<<table<<endl;
				//catalog.DropTable(table);
				break;			
			case'3':
				cout<<"DISPLAY CATALOG"<<endl;
				cout<<"displaying catalog"<<endl;
				//catalog
				break;
			case'q':
				cout<<"Are you sure? (y\\n)"<<endl;
				cin>>in;
				if(in == 'y')return 0;
				break;
			default:
				cout<<"Not an input option"<<endl;
				break;
		}
		cout<<"\n\tMENU"<<endl;
		cout<<"1)CREATE TABLE\n2)DROP TABLE\n3)DISPAY CATALOG\n\n q: quit"<<endl;
		cin>>in;
	}
	
	return 0;
}