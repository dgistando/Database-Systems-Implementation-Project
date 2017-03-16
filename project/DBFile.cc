#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName("") {
    page.EmptyItOut();
    pageNum = 0;
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
    int retval  = 0;
    if(f_type == Heap){
        fileName = string(f_path);
        fileType = f_type;
        retval =  file.Open(0,f_path); // zero coz creating
        pageNum = file.Close();
    }
    return retval;
}

int DBFile::Open (char* f_path) {
    fileName = f_path;
    return file.Open(1,f_path);
}

void DBFile::Load (Schema& schema, char* textFile) {
    FILE * fileToRead = fopen(textFile,"r");
    int i = 0;
    while (1) {
        Record rec;
        if (rec.ExtractNextRecord (schema, *fileToRead) == 0) {
            break;
        }
        i++;
        AppendRecord(rec);
    } fclose(fileToRead);
    //file.AddPage(page, file.GetLength());
    cout << "\n records: " << i << " pages: " << file.GetLength() << endl;
//    FILE *fileTable = fopen(textFile , "r");
//    Record tempRec;
//    Page tempPage;
//    file.GetPage(tempPage,pageNum);
//    int i = 0;
//    while(1) {
//        if(tempRec.ExtractNextRecord(schema, *fileTable) == 1) {
//            if(tempPage.Append(tempRec) == 0) {
//                    file.AddPage(tempPage, pageNum);
//                    pageNum++;
//                    Page bufferPage; bufferPage.EmptyItOut();
//                    file.AddPage(bufferPage, pageNum);
//                    file.GetPage(tempPage, pageNum);
//                    tempPage.Append(tempRec);
//            }
//        } else {
//            file.AddPage(tempPage,pageNum);
//            break;
//        }
//        i++;	
//    }
//    cout << "\n records: " << i << " pages: " << file.GetLength() << endl;
    MoveFirst();
}

int DBFile::Close () {
    return file.Close();
}

void DBFile::MoveFirst () {
    //cout << "\n  Old Page: " << pageNum << endl;
    pageNum = 0;
    //cout << "\n  New Page: " << pageNum << endl;
}

void DBFile::AppendRecord (Record& rec) {
    if (page.Append(rec) == 0) {
        file.AddPage(page, pageNum);
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
        //cout << " Current Page: " << pageNum << endl;
    } return 1;
    
}

// this function retrieves tableName from fileName
// fileName is currently (as of phase 2) same as tableName
// this should be changed if we change our naming rule for the fileName
string DBFile::GetTableName() {
	return fileName;
}
off_t DBFile :: GetPageNums () {
	return file.GetLength();
}
void DBFile:: SetPageNums(int num){
    file.SetPageNums(num);
}