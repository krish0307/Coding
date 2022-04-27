#include <iostream>
#include <fstream>
#include <cstring>
#include <string>
#include "Schema.h"
#include "TwoWayList.h"
#include "File.h"
#include "Defs.h"
#include "ComparisonEngine.h"
#include "Record.h"
#include "Comparison.h"
#include "DBFile.h"

DBFile :: DBFile () {}
DBFile :: ~DBFile () {
    delete genericDBFile;
}

int DBFile :: Create (const char *fpath, fType ftype, void *startup) {
    char metaPath[100];
    sprintf (metaPath, "%s.md", fpath);
    ofstream metaStream;
    metaStream.open (metaPath);

    switch(ftype){
        case sorted:{
            metaStream << "sorted" << endl;
            SortInfo* sortInfoInstance = (SortInfo *) startup;
            metaStream << sortInfoInstance->runLength << endl;
            metaStream << sortInfoInstance->myOrder->numAtts << endl;
            sortInfoInstance->myOrder->PrintInOfstream (metaStream);
            genericDBFile = new DBSortedFile (sortInfoInstance->myOrder, sortInfoInstance->runLength);
            break;
        }
        case heap:{
            metaStream << "heap" << endl;
            genericDBFile = new DBHeapFile;
            break;
        }
        default:{
            metaStream.close ();
            cout << "Not a valid file Type: " << fpath << endl;
            return 0;}
    }
    metaStream.close ();
    genericDBFile->Create (fpath);
    return 1;
}

int DBFile :: Open (char *fpath) {
    char *metaPath = new char[100];
    sprintf (metaPath, "%s.md", fpath);
    ifstream metaInStream;
    metaInStream.open (metaPath);
    string str;
    int attNum = 0;

    if (metaInStream.is_open ()) {
        metaInStream >> str;
        if ( str == "heap") {
            genericDBFile = new DBHeapFile;
        } else if (str == "sorted"){
            OrderMaker *orderMaker = new OrderMaker;
            int runL;
            metaInStream >> runL;
            metaInStream >> orderMaker->numAtts;
            int i = 0;
            while(i<orderMaker->numAtts){
                metaInStream >> attNum;
                metaInStream >> str;
                orderMaker->whichAtts[i] = attNum;
                if (str == "Int" ) {
                    orderMaker->whichTypes[i] = Int;
                }  else if ( str == "String" ) {
                    orderMaker->whichTypes[i] = String;
                } else if (str == "Double" ) {
                    orderMaker->whichTypes[i] = Double;
                }else {
                    cout << "Not a valid metadata for sorted File: " << fpath << endl;
                    delete orderMaker;
                    metaInStream.close ();
                    return 0;
                }
                i++;
            }
            genericDBFile = new DBSortedFile (orderMaker, runL);
        }  else {
            metaInStream.close ();
            cout << "Not a valid file type in metadata for (" << fpath << ")" << endl;
            return 0;
        }

    } else {
        metaInStream.close ();
        cout << "Not able to open file (" << fpath << ")!!" << endl;
        return 0;
    }
    genericDBFile->Open (fpath);
    metaInStream.close ();
    return 1;
}


void DBFile :: Load (Schema &myschema, const char *loadpath) {
    genericDBFile->Load (myschema, loadpath);
}

int DBFile :: Close () {
    return genericDBFile->Close ();
}

void DBFile :: Add (Record &addme) {
    genericDBFile->Add (addme);
}

void DBFile :: MoveFirst () {
    genericDBFile->MoveFirst ();
}

int DBFile :: GetNext (Record &fetchme) {
    return genericDBFile->GetNext (fetchme);
}

int DBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return genericDBFile->GetNext (fetchme, cnf, literal);
}

DBHeapFile :: DBHeapFile () {
    currPage = new Page ();
    file = new File ();
}

int DBHeapFile :: Create (const char *fpath) {
    this->filePath = (char *)fpath;
    strcpy (this->filePath, (char *)fpath);
    currPgIdx = 0;
    currPage->EmptyItOut ();
    file->Open (0, this->filePath);
    writeMode = true;
    return 1;

}

void DBHeapFile :: Load (Schema &f_schema, const char *loadpath) {
    FILE *inputFileInstance = fopen((char *)loadpath, "r");
    if(inputFileInstance == 0){
        exit(EXIT_FAILURE);
    }
    int counterOfRecords =0, counterOfPages =0;
    Record recInstance;
    Page pageInstance;
    while(recInstance.SuckNextRecord(&f_schema, inputFileInstance)==1){
        counterOfRecords++;
        if(pageInstance.Append(&recInstance)==0){    
            file->AddPage(&pageInstance, counterOfPages++); 
            pageInstance.EmptyItOut();  
            pageInstance.Append(&recInstance);   
        }
    }
    file->AddPage(&pageInstance, counterOfPages++);  
    cout<< "Fetched "<< counterOfRecords << " records, into "<< counterOfPages <<" pages. "<< endl;
}

int DBHeapFile :: Open (char *fpath) {
    this->filePath = (char *)fpath;
    file->Open (1, (char *)fpath);
    currPage->EmptyItOut ();
    currPgIdx = (off_t) 0;
    return 1;
}

int DBHeapFile :: Close () {
    if (writeMode && currPage->GetNumRecs () > 0) {
        FlushCurrentPage();
        writeMode = false;
    }
    file->Close ();
    return 1;
}

void DBHeapFile :: MoveFirst () {
    if (writeMode && currPage->GetNumRecs () > 0) {
        FlushCurrentPage();
        writeMode = !writeMode;
    }
    currPage->EmptyItOut ();
    currPgIdx = (off_t) 0;
    file->GetPage (currPage, currPgIdx);
}

void DBHeapFile :: Add (Record &rec) {
    if (! (currPage->Append (&rec))) {
        FlushCurrentPage();
        currPage->Append (&rec);
    }
}

int DBHeapFile :: GetNext (Record &fetchme) {
    while (!currPage->GetFirst(&fetchme)) {
        if (currPgIdx == file->GetLength() - 2) {     
            return 0;
        }
        else {
            file->GetPage(currPage, ++ currPgIdx);   
        }
    }
    return 1;
}

int DBHeapFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comparisonEngine;
    while(GetNext(fetchme) == 1)
    {
        if (comparisonEngine.Compare(&fetchme,&literal,&cnf))
            return 1;
    }
    return 0;
}

void DBHeapFile :: FlushCurrentPage() {
    file->AddPage (currPage, currPgIdx++);
    currPage->EmptyItOut ();
}

DBHeapFile :: ~DBHeapFile () {
    delete file;
    delete currPage;
}

DBSortedFile :: DBSortedFile (OrderMaker *order, int runLen) {
    this->order = order;
    this->runLength = runLen;
    this->query = NULL;
    this->currPage = new Page ();
    this->bigq = NULL;
    this->file = new File ();
    this->buffsize = 100;
}

DBSortedFile :: ~DBSortedFile () {
    delete currPage, file, query;
}

int DBSortedFile :: Create (const char *fpath) {
    this->filePath = (char *)fpath;
    currPage->EmptyItOut ();
    file->Open (0, this->filePath);
    writeMode = false;
    currPgIdx = 0;
    return 1;
}

int DBSortedFile :: Open (char *fpath) {
    currPgIdx = (off_t)0;
    this->filePath = (char *)fpath;
    currPage->EmptyItOut ();
    writeMode = false;
    file->Open (1, (char *)fpath);
    if (file->GetLength () > 0) {
        file->GetPage (currPage, currPgIdx);
    }
    return 1;
}

int DBSortedFile :: Close () {
    if (writeMode == true)
        TwoWayMerge ();
    return file->Close ();
}

void DBSortedFile :: Add (Record &addme) {
    if (writeMode == false)
        InitBigQ ();
    inPipe->Insert (&addme);
}

void DBSortedFile :: Load (Schema &myschema, const char *loadpath) {
    FILE *inputFileInstance = fopen((char *)loadpath, "r");
    if(inputFileInstance == 0){
        exit(EXIT_FAILURE);
    }
    int recordsCount =0, pagesCount =0;
    Record recordInstance;
    Page pageInstance;
    while(recordInstance.SuckNextRecord(&myschema, inputFileInstance)==1){
        recordsCount++;
        if(pageInstance.Append(&recordInstance)==0){    
            file->AddPage(&pageInstance, pagesCount++);  
            pageInstance.EmptyItOut();  
            pageInstance.Append(&recordInstance);   
        }
    }
    file->AddPage(&pageInstance, pagesCount++); 
}

void DBSortedFile :: MoveFirst () {
    if (writeMode == true) {
        TwoWayMerge ();
    } else {
        currPgIdx = 0;
        currPage->EmptyItOut ();
        file->GetPage (currPage, currPgIdx);
        if (query!=NULL)
            delete query;
    }
}

int DBSortedFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    if ( writeMode == true ) {
        TwoWayMerge ();
    }
    if (!query) {
        query = new OrderMaker;
        return QueryOrderMaker (*query, *order, cnf) > 0 ?  BinarySearch (fetchme, cnf, literal) : GetNextInSequence (fetchme, cnf, literal);
    } else {
        return query->numAtts == 0? GetNextInSequence (fetchme, cnf, literal): GetNextOfQuery (fetchme, cnf, literal);
    }
}

int DBSortedFile :: GetNext (Record &fetchme) {
    if (writeMode == true)
        TwoWayMerge ();
    if (!currPage->GetFirst(&fetchme)) {
        if (++currPgIdx < file->GetLength() - 1) {    
            file->GetPage(currPage, currPgIdx);
            currPage->GetFirst(&fetchme);
        }
        else {
            return 0;
        }
    }
    return 1;
}


int DBSortedFile :: GetNextOfQuery (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine cmprEngine;
    while (GetNext (fetchme) && !cmprEngine.Compare (&literal, query, &fetchme, order)) {
        if (cmprEngine.Compare (&fetchme, &literal, &cnf))
            return 1;
    }
    return 0;
}

int DBSortedFile :: GetNextInSequence (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine cmprEngine;
    while (GetNext (fetchme) == 1) {
        if (cmprEngine.Compare (&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}

int DBSortedFile :: BinarySearch(Record &fetchme, CNF &cnf, Record &literal) {
    off_t midVal = 0;
    off_t firstVal = currPgIdx;
    off_t lastVal = file->GetLength () - 1;
    Page *pageInstance = new Page;
    ComparisonEngine cmprEngine;
    while (true) {
        if(lastVal > firstVal) {
            midVal = firstVal + (lastVal - firstVal) / 2;
            file->GetPage(pageInstance, midVal);

            if (pageInstance->GetFirst(&fetchme)) {

                if (cmprEngine.Compare(&literal, query, &fetchme, order) > 0) {
                    firstVal = ++midVal;
                } else {
                    lastVal = --midVal;
                }
            } else
                break;
        }
        else
            break;
    }

    if (cmprEngine.Compare (&fetchme, &literal, &cnf)) {
        currPage = pageInstance;
        currPgIdx = midVal;
        delete currPage;
        return 1;

    } else {
        delete pageInstance;
        return 0;
    }

}

void DBSortedFile :: InitBigQ () {
    inPipe = new Pipe (buffsize);
    outPipe = new Pipe (buffsize);
    bigq = new BigQ(*inPipe, *outPipe, *order, runLength);
    writeMode = true;
}

void DBSortedFile :: TwoWayMerge () {
    inPipe->ShutDown ();
    Record *pipeRec = new Record;
    Record *fileRec = new Record;
    writeMode = false;
    if (file->GetLength () > 0) {
        MoveFirst ();
    }

    int flagPipe = outPipe->Remove (pipeRec);
    int flagFile = GetNext (*fileRec);
    DBHeapFile *newFileInstance = new DBHeapFile;
    newFileInstance->Create ("temp.bin");
    ComparisonEngine cmprEngine;
    while (flagFile && flagPipe) {
        if (cmprEngine.Compare (pipeRec, fileRec, order) <= 0) {
            newFileInstance->Add (*pipeRec);
            flagPipe = outPipe->Remove (pipeRec);
        } else {
            newFileInstance->Add (*fileRec);
            flagFile = GetNext (*fileRec);
        }
    }
    for(;flagFile!=NULL;flagFile = GetNext(*fileRec))
        newFileInstance->Add (*fileRec);
    for(;flagPipe!=NULL;flagPipe = outPipe->Remove (pipeRec))
        newFileInstance->Add (*pipeRec);

    newFileInstance->Close ();
    delete newFileInstance;
    outPipe->ShutDown ();
    remove (filePath);
    rename ("temp.bin", filePath);
    file->Close ();
    file->Open (1, filePath);
    MoveFirst ();
}

int DBSortedFile :: QueryOrderMaker (OrderMaker &query, OrderMaker &order, CNF &cnf) {
    bool boolFlag = false;
    query.numAtts = 0;
    int first=0,second=0;
    while(first<order.numAtts){
        first++; second=0;
        while(second< cnf.numAnds){
            second++;
            if (cnf.orLens[second] != 1 || cnf.orList[second][0].op != Equals || (cnf.orList[first][0].operand1 == Left && cnf.orList[first][0].operand2 == Left) ||
                (cnf.orList[first][0].operand1==Left && cnf.orList[first][0].operand2 == Right) ||
                (cnf.orList[first][0].operand1==Right && cnf.orList[first][0].operand2 == Left) ||
                (cnf.orList[first][0].operand2 == Right && cnf.orList[first][0].operand1 == Right)) {
                continue;
            }
            if (cnf.orList[second][0].operand1 == Left &&
                cnf.orList[second][0].whichAtt1 == order.whichAtts[first]) {
                query.whichAtts[query.numAtts] = cnf.orList[first][0].whichAtt2;
                query.whichTypes[query.numAtts++] = cnf.orList[first][0].attType;
                boolFlag = true;
                break;
            }
            if (cnf.orList[second][0].operand2 == Left &&
                cnf.orList[second][0].whichAtt2 == order.whichAtts[first]) {
                query.whichAtts[query.numAtts] = cnf.orList[first][0].whichAtt1;
                query.whichTypes[query.numAtts++] = cnf.orList[first][0].attType;
                boolFlag = true;
                break;
            }
        }
        if (boolFlag == false) {
            break;
        }
    }
    return query.numAtts;

}
GenericDBFile :: ~GenericDBFile () {}