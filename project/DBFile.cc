#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName("") {
	pageCount = 0;
        page.EmptyItOut();
        isOpen = false;
}

DBFile::~DBFile () {
    isOpen = false;
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName), pageCount(0) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
    // handle self-assignment first
    if (this == &_copyMe) return *this;

    file = _copyMe.file;
    fileName = _copyMe.fileName;
    pageCount = _copyMe.pageCount;
    isOpen = _copyMe.isOpen;
    //page = _copyMe.page;

    return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {

    if (f_type == Heap) {	
        ftype = f_type;
        fileName = f_path;
        isOpen = true;
        return (file.Open(0,f_path));
    }
}

int DBFile::Open (char* f_path) {
	fileName = f_path;
        isOpen = true;
	return file.Open(1,f_path);

}

void DBFile::Load (Schema& schema, char* textFile) {
    FILE * pFile;
    string str = textFile;
    pFile = fopen(&str[0],"r");
    int i = 0;
    while (1) {
        Record rec;
        if (rec.ExtractNextRecord (schema, *pFile) == 0) break;
        AppendRecord(rec);
        i++;
    }
    pageCount++;
    file.AddPage(page, file.GetLength());
    fclose(pFile);
    cout << "\n\n enties read: " << i << " pages: " << pageCount << endl;;
}

int DBFile::Close () {
    isOpen = false;
    return file.Close();
}

void DBFile::MoveFirst () {
	pageCount = 0;
	file.GetPage(page,pageCount);
}

void DBFile::AppendRecord (Record& rec) {

    if (page.Append(rec) == 0) {
        file.AddPage(page, file.GetLength());
        page.EmptyItOut();
        page.Append(rec);
        pageCount++;
    }
}

int DBFile::GetNext (Record& rec) {
   // if(!isOpen){Open(&fileName[0]); MoveFirst(); }
    if (page.GetFirst(rec) == 0) {
        pageCount++;
        //cout << "\n jump to page: " << pageCount << endl;
        page.EmptyItOut();
        if (file.GetLength() == pageCount) return 0;
        if (file.GetPage(page, pageCount) == -1) return 0;
        if(page.GetFirst(rec) == 0){ return GetNext(rec); }
    } return 1;	
}

