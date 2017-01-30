#include <vector>
#include <string>
#include <iostream>

#include "Schema.h"
#include "Catalog.h"

using namespace std;
int printMenu (){
	int answer;
	cout << "MENU: " << endl;
	cout << endl;
	cout << "1) CREATE TABLE" << endl;
	cout << "2) DROP TABLE" << endl;
	cout << "3) VIEW CATALOG" << endl;
	cout << "4) SAVE DATABSE" << endl;
	cin >> answer;
	switch(answer){
		case(answer = 1):
			CreateTable();
			break;
		case(answer = 2):
			DropTable();
			break;
		case(answer = 3):
			//multiple functions here?
			break;
		case(answer = 4):
			Save();
			break;
	}
}

int main () {
	string db_name = "";
	cout << "Enter the database: ";
	cin >> db_name;
	if(db_name != ""){
		Catalog(*db_name);
//<<<<<<< HEAD
	//drop a table,
//=======
	}
	printMenu();
//>>>>>>> refs/remotes/origin/master


	return 0;
}

	/*string table = "region", attribute, type;
	vector<string> attributes, types;
	vector<unsigned int> distincts;

	attribute = "r_regionkey"; attributes.push_back(attribute);
	type = "INTEGER"; types.push_back(type);
	distincts.push_back(5);
	attribute = "r_name"; attributes.push_back(attribute);
	type = "STRING"; types.push_back(type);
	distincts.push_back(5);
	attribute = "r_comment"; attributes.push_back(attribute);
	type = "STRING"; types.push_back(type);
	distincts.push_back(5);
>>>>>>> 896694c1687c4835843ebce474e6e864a715c0aa:project/main.cc

attribute = "r_regionkey"; attributes.push_back(attribute);
type = "INTEGER"; types.push_back(type);
distincts.push_back(5);
attribute = "r_name"; attributes.push_back(attribute);
type = "STRING"; types.push_back(type);
distincts.push_back(5);
attribute = "r_comment"; attributes.push_back(attribute);
type = "STRING"; types.push_back(type);
distincts.push_back(5);

Schema s(attributes, types, distincts);
Schema s1(s), s2; s2 = s1;

string a1 = "r_regionkey", b1 = "regionkey";
string a2 = "r_name", b2 = "name";
string a3 = "r_commen", b3 = "comment";

s1.RenameAtt(a1, b1);
s1.RenameAtt(a2, b2);
s1.RenameAtt(a3, b3);

s2.Append(s1);

vector<int> keep;
keep.push_back(5);
keep.push_back(0);
s2.Project(keep);

cout << s << endl;
cout << s1 << endl;
cout << s2 << endl;


string dbFile = "catalog.sqlite";
Catalog c(dbFile);

<<<<<<< HEAD:main.cc
c.CreateTable(table, attributes, types);

cout << c << endl;*/
