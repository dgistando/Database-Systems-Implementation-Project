#ifndef DBFILE_H
#define DBFILE_H

#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"

using namespace std;


class DBFile {
private:
	File file;
	string fileName;
        
        Page page;
        int pageNum;
        FileType fileType;

public:
	DBFile ();
	virtual ~DBFile ();
	DBFile(const DBFile& _copyMe);
	DBFile& operator=(const DBFile& _copyMe);

	int Create (char* fpath, FileType file_type);
	int Open (char* fpath);
	int Close ();

	void Load (Schema& _schema, char* textFile);

	void MoveFirst ();
	void AppendRecord (Record& _addMe);
	int GetNext (Record& _fetchMe);

	// this function retrieves tableName from fileName
	// fileName is currently (as of phase 2) same as tableName
	// this should be changed if we change our naming rule for the fileName
	string GetTableName();
        off_t GetPageNums();
        void SetPageNums(int num);
};

#endif //DBFILE_H
