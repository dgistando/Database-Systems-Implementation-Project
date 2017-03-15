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
    if(f_type == Heap){
        fileName = string(f_path);
        fileType = f_type;
        return file.Open(0,f_path); // zero coz creating
    }
}

int DBFile::Open (char* f_path) {
    fileName = f_path;
    return file.Open(1,f_path);
}

void DBFile::Load (Schema& schema, char* textFile) {
    FILE * fileToRead = fopen(textFile,"r");
    while (1) {
        Record rec;
        if (rec.ExtractNextRecord (schema, *fileToRead) == 0) break;
        AppendRecord(rec);
    } fclose(fileToRead);
    file.AddPage(page, file.GetLength());
}

int DBFile::Close () {
    return file.Close();
}

void DBFile::MoveFirst () {
    pageNum = 0;
}

void DBFile::AppendRecord (Record& rec) {
    if (page.Append(rec) == 0) {
        file.AddPage(page, file.GetLength());
        page.EmptyItOut();
        page.Append(rec);
        pageNum++;
    }
}

int DBFile::GetNext (Record& rec) {
    if (page.GetFirst(rec) == 0) {
        if (file.GetLength() == pageNum) return 0;
        if (file.GetPage(page, pageNum) == -1) return 0;
        page.GetFirst(rec);
        pageNum++;
    } return 1;
    
}

// this function retrieves tableName from fileName
// fileName is currently (as of phase 2) same as tableName
// this should be changed if we change our naming rule for the fileName
string DBFile::GetTableName() {
	return fileName;
}