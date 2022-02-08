#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "iostream"
using namespace std;

DBFile::DBFile () {
    curRec = new Record();
    endOfFile = 0;
    pageIndex = 0;
    additionalPage = 0;
}

//This method is used in creating the binary files.
int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    if(f_type==heap){
    file.Open(0,(char *) f_path);
    return 1;
    }
    else{
        return 0;
    }
}

//Used to load DBFile instance from a txt file.
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE* fileToLoad = fopen (loadpath,"r");
    while(curRec->SuckNextRecord(&f_schema,fileToLoad)!=0){
        Add(*curRec);
    }
    if(additionalPage==1){
        file.AddPage(&bufPage,pageIndex);
        pageIndex++;
        bufPage.EmptyItOut();
        additionalPage=0;
    }
    fclose(fileToLoad);
}

//Used to open a existing bin file
int DBFile::Open (const char *f_path) {
    file.Open(1,(char *) f_path);
    pageIndex=0;
    endOfFile=0;
    return 1;
}

void DBFile::MoveFirst () {
    file.GetPage(&bufPage,0);
}

//Used to close the dbfile
int DBFile::Close () {
    endOfFile=1;
    file.Close();
    return 1;
}

//This method adds the record to the end of the file
void DBFile::Add (Record &rec) {
    additionalPage=1;
    if(bufPage.Append(&rec)==0){
        file.AddPage(&bufPage,pageIndex);
        pageIndex++;
        bufPage.EmptyItOut();
        bufPage.Append(&rec);
    }
}

//This method gets the next record in the file
int DBFile::GetNext (Record &fetchme) {
    if(bufPage.GetFirst(&fetchme)==0){
        pageIndex++;
        if(pageIndex>=file.GetLength()-1){
            endOfFile=1;
            return 0;
        }
        else{
            file.GetPage(&bufPage,pageIndex);
            bufPage.GetFirst(&fetchme);
            return 1;
        }        
    }
    else{
        return 1;
    }
}

//This method takes a CNF parameter based on which the next record in the file is selected
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine engine;
    if(GetNext(fetchme)==0){
        return 0;
    }
    while(engine.Compare(&fetchme,&literal,&cnf)==0){
        if(GetNext(fetchme)==0){
        return 0;
    }
    }
    return 1;
}