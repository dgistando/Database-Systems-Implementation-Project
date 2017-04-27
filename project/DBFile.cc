#include <sys/stat.h>
#include <string>
#include <vector>
#include <sstream>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName(""), isMovedFirst(false) {}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName), isMovedFirst(false) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {
	fileName = f_path;
	fileType = f_type;

        
        isOpen = true;
	// return 0 on success, -1 otherwise
	return file.Open(0, f_path); // mode = O_TRUNC | O_RDWR | O_CREAT;
}

int DBFile::Open (char* f_path) {
	fileName = f_path;
	
        isOpen = true;
        
	// check whether file exists or not
	struct stat fileStat;
	if(stat(f_path, &fileStat) != 0) {
		return Create(f_path, Heap);
	} else {
		// return 0 on success, -1 otherwise
		return file.Open(fileStat.st_size, f_path); // mode = O_RDWR;
	}
}

void DBFile::Load (Schema& schema, char* textFile) {
	MoveFirst();
        cout<< "FileName: "<< textFile<<endl;
	FILE* textData = fopen(textFile, "r");

	while(true) {
		Record record;
		if(record.ExtractNextRecord(schema, *textData)) { // success on extract
			AppendRecord(record);
		} else { // no data left or error
			break;
		}
	}
	file.AddPage(pageNow, iPage++); // add the last page to the file
	fclose(textData);
}

int DBFile::Close () {
        isOpen = false;
	return file.Close();
}

void DBFile::MoveFirst () {
	iPage = 0; // reset page index to 0
	isMovedFirst = true;
	pageNow.EmptyItOut(); // the first page has no data
}

void DBFile::AppendRecord (Record& rec) {
	if(!pageNow.Append(rec)) { // no space in the current page, pageNow
		file.AddPage(pageNow, iPage++); // add pageNow to the file
		pageNow.EmptyItOut(); // clear pageNow
		pageNow.Append(rec); // add rec to the pageNow
	}
}

int DBFile::GetNext (Record& rec) {
	if(!isMovedFirst) {
		MoveFirst();
	}
	
	off_t numPage = file.GetLength();
	while(true) {
		if(!pageNow.GetFirst(rec)) { // no record in the current page
			// check whether this is the last page
			if(iPage == numPage) { // EOF
				break;
			} else { // move on to the next page
				file.GetPage(pageNow, iPage++);
			}
		} else { // record exists
			return 1;
		}
	}
	return 0;
}

string DBFile::GetTableName() {
	vector<string> path;
	stringstream ss(fileName); string tok;
	while(getline(ss, tok, '/')) {
		path.push_back(tok);
	}
	string tempFileName = path[path.size()-1];
	stringstream ss2(tempFileName); string tableName = "";
	getline(ss2, tableName, '.');
	return tableName;
}

/*#include <string>

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
        MoveFirst();
	return file.Open(1,&fileName[0]);
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
    if(ftype == Sorted) {
        file.AddPage(page, file.GetLength());
    } // testing
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
//    if(!isOpen){Open(&fileName[0]); MoveFirst(); }
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
    
    
    
    if(page.GetFirst(rec) == 0){
        pageCount++;
        if(pageCount <= file.GetLength()){
            //page.EmptyItOut();
           // pageCount++;
            file.GetPage(page,pageCount - 1);
            //pageCount++;
            return page.GetFirst(rec);
        } else return 0;
    } return 1;
    
    /*if(page.GetFirst(rec))return true;
    
    cout<<"FILE LENGTH: "<< file.GetLength() <<endl;   
    cout<<"PAGE COUNTS: "<< pageCount <<endl;    
    //If not last record
    if(pageCount < file.GetLength()){
        file.GetPage(page, ++pageCount);
        return page.GetFirst(rec);
    }
    return false;
    
//    if (page.GetFirst(rec) == 0) {
//        if (file.GetLength() == pageCount) return 0;
//        if (file.GetPage(page, pageCount) == -1) return 0;
//        page.GetFirst(rec);
//        pageCount++; }
//	return 1;
}
int DBFile::GetSpecificRecord(int pNumber, int rNumber, Record& rec) {
    page.EmptyItOut(); 
    if (file.GetPage(page, pNumber) == -1) return 0; // return 0 for no page found 
            //page.GetFirst(rec); return 0; 
    if (page.GetRecordNumber(rNumber, rec) != 1) return 0; 
    return 1; // return 1 if record exists 
}
*/