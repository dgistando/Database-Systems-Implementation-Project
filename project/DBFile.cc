#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName("") {
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {
	// this is just for phase2, to retrieve table name
	fileName = string(f_path);
}

int DBFile::Open (char* f_path) {
}

void DBFile::Load (Schema& schema, char* textFile) {
}

int DBFile::Close () {
}

void DBFile::MoveFirst () {
}

void DBFile::AppendRecord (Record& rec) {
}

int DBFile::GetNext (Record& rec) {
}

// this function retrieves tableName from fileName
// fileName is currently (as of phase 2) same as tableName
// this should be changed if we change our naming rule for the fileName
string DBFile::GetTableName() {
	return fileName;
}