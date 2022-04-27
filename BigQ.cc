#include "BigQ.h"

RecordComparer :: RecordComparer (OrderMaker* sort_order) : sortordering(sort_order){
}

bool RecordComparer::operator() (Record* leftRecord, Record* rightRecord) {
    ComparisonEngine cmprEngine;
    return (cmprEngine.Compare (leftRecord, rightRecord, sortordering) < 0) ;
}

bool RunComparer :: operator() (Run* leftRun, Run* rightRun) {
    ComparisonEngine cmprEngine;
    return (cmprEngine.Compare (leftRun->headRecordInstance, rightRun->headRecordInstance, leftRun->sortordering) >= 0);
}

Run :: ~Run () {
    delete headRecordInstance;
}

Run :: Run (int length, int offset, File* runFileName, OrderMaker* sort_order) : pageSizeRun(length), pageOffSetRun(offset), fileRun(runFileName), sortordering(sort_order) {

    headRecordInstance = new Record ();
    fileRun->GetPage (&pageRun, offset);
    getHead ();

}

Run :: Run (File* runFileName, OrderMaker* sort_order) : headRecordInstance(NULL), fileRun(runFileName), sortordering(sort_order){
}

int Run :: getHead () {
    Record* tmpRec = new Record();
    if(pageSizeRun <= 0) {return 0;}

    if (pageRun.GetFirst(tmpRec) == 0) {
        fileRun->GetPage(&pageRun, ++pageOffSetRun);
        pageRun.GetFirst(tmpRec);
    }

    pageSizeRun--;
    headRecordInstance->Consume(tmpRec);
    return 1;
}

void BigQ :: WriteToFile () {

    Page* tmpPg = new Page();
    int recordCount = recordVec.size(), headOffset = pageCounter, numPages = 1;

    for (auto tempRecord : recordVec) {
        if ((tmpPg->Append (tempRecord)) == 0) {
            numPages++;
            runsFile.AddPage (tmpPg, pageCounter++);
            tmpPg->EmptyItOut ();
            tmpPg->Append (tempRecord);
        }
    }
    runsFile.AddPage(tmpPg, pageCounter++);
    tmpPg->EmptyItOut();
    delete tmpPg;
    recordVec.clear();
    AddRunToHeap (recordCount, headOffset);
}

void BigQ :: AddRunToHeap (int runPageSize, int runPageOffset) {
    Run* tmpRun = new Run(runPageSize, runPageOffset, &runsFile, sortordering);
    runHeap.push(tmpRun);
    return;
}

void BigQ :: SortPhase() {

    writeFileName = new char[100];
    Record* tmpRec = new Record();
    Page* tmpPg = new Page();

    srand (time(NULL));
    sprintf (writeFileName, "%d.txt", (uintptr_t)workerThread);

    runsFile.Open (0, writeFileName);

    int pgCount = 0, numRecs = 0;
    while (inP->Remove(tmpRec)) {

        Record* recVec = new Record ();
        recVec->Copy (tmpRec);
        numRecs++;
        if (tmpPg->Append (tmpRec) == 0) {
            if (++pgCount == runlength) {
                sort (recordVec.begin (), recordVec.end (), RecordComparer (sortordering));
                WriteToFile();
                pgCount = 0;
            }
            tmpPg->EmptyItOut ();
            tmpPg->Append (tmpRec);
        }
        recordVec.push_back(recVec);
    }
    if(!recordVec.empty()) {
        sort (recordVec.begin (), recordVec.end (), RecordComparer(sortordering));
        WriteToFile();
        tmpPg->EmptyItOut ();
    }
    delete tmpRec;
    delete tmpPg;

}

void BigQ :: MergePhase () {
    Run* tmpRun = new Run (&runsFile, sortordering);
    while (!runHeap.empty ()) {
        Record* tmpRec = new Record ();
        tmpRun = runHeap.top();
        tmpRec->Copy(tmpRun->headRecordInstance);
        outP->Insert(tmpRec);
        runHeap.pop ();
        if (tmpRun->getHead() > 0) {
            runHeap.push(tmpRun);
        }
        delete tmpRec;
    }
    runsFile.Close();
    outP->ShutDown();
    delete tmpRun;
    remove(writeFileName);
}

BigQ :: BigQ (Pipe& in, Pipe& out, OrderMaker& sort_order, int length) : sortordering(&sort_order), inP(&in), outP(&out), runlength(length), pageCounter(1){
    pthread_create(&workerThread, NULL, StartMainThread, (void*)this);
}