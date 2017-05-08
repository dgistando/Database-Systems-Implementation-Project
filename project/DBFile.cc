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
	file(_copyMe.file),	fileName(_copyMe.fileName), isOpen(_copyMe.isOpen), pageCount(0) {}

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

    //if (f_type == Heap) {	
        ftype = f_type;
        fileName = f_path;
        isOpen = true;
        return (file.Open(0,f_path));
    //}
}

int DBFile::Open (char* f_path) {
	fileName = f_path;
        isOpen = true;
	return file.Open(1,f_path);

}
int DBFile::Open () {
        isOpen = true;
	int result = file.Open(1,&fileName[0]);
        MoveFirst();
        return result;
}

void DBFile::Load (Schema& schema, char* textFile) {
    FILE * pFile = fopen(textFile,"r");
    int i = 0;
    while (1) {
        Record rec;
        if (rec.ExtractNextRecord (schema, *pFile) == 0) { break; }
        else { AppendRecord(rec); i++; }
        
    }
    //cout << "After loop append page count: " << pageCount << endl;
    file.AddPage(page, pageCount);
    fclose(pFile);
    cout << "\n\n enties read: " << i << " pages: " << pageCount << endl;;
}

int DBFile::Close () {
    isOpen = false;
    if(ftype == Sorted) { 
    file.AddPage(page, pageCount); } // testing
    return file.Close();
}

void DBFile::MoveFirst () {
	pageCount = 0;
        page.EmptyItOut();
	file.GetPage(page,pageCount);
}

void DBFile::AppendRecord (Record& rec) {

    if (page.Append(rec) == 0) {
        //cout << "Making new page after " << pageCount << endl;
        file.AddPage(page, pageCount++);
        page.EmptyItOut();
        page.Append(rec);
    }
}

int DBFile::GetNext (Record& rec) {
    if(page.GetFirst(rec) == 0){
        if(pageCount < file.GetLength()){
                
                pageCount++;
                file.GetPage(page, pageCount);
                return page.GetFirst(rec);
        }else{ return 0; } } return 1;
    
//    int size = file.GetLength();
//    while(1){
//        if(!page.GetFirst(rec)){
//            if(pageCount == size){ break; } // end of file
//            else { file.GetPage(page, ++pageCount); } //move to next page
//        } else { return 1; } // record found
//    } return 0;
    
//    //if(!isOpen){Open(&fileName[0]); MoveFirst(); }
//    if (page.GetFirst(rec) == 0) {
//        pageCount++;
//        //cout << "\n jump to page: " << pageCount << endl;
//        page.EmptyItOut();
//        if (file.GetLength() == pageCount) return 0;
//        if (file.GetPage(page, pageCount) == -1) return 0;
//        //return page.GetFirst(rec);
//        if(page.GetFirst(rec) == 0)
//        { return GetNext(rec); }
//    } return 1;
    
}
void DBFile::GetPageNo (int number, Page& indexpage) { file.GetPage(indexpage, number); }
int DBFile::GetSpecificRecord(int pNumber, int rNumber, Record& rec) {
    page.EmptyItOut(); 
    if (file.GetPage(page, pNumber) == -1) return 0; // return 0 for no page found 
            //page.GetFirst(rec); return 0; 
    if (page.GetRecordNumber(rNumber, rec) != 1) return 0; 
    return 1; // return 1 if record exists 
}
