#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class Run;

class RunComparer {
private:
    OrderMaker* sortOrderMaker;
public:
    bool operator() (Run* leftRun, Run* rightRun);
};

class BigQ {

private:
    int pageCounter;
    pthread_t workerThread;
    char* writeFileName;
    priority_queue<Run*, vector<Run*>, RunComparer> runHeap;
    OrderMaker* sortordering;
    void WriteToFile ();
    Pipe* inP;
    Pipe* outP;
    void AddRunToHeap (int runPageSize, int runPageOffset);
    friend bool RecordComparer (Record* leftRecord, Record* rightRecord);

public:

    int runlength;
    vector<Record*> recordVec;
    File runsFile;

    void SortPhase();
    void MergePhase ();

    static void *StartMainThread (void* start) {
        BigQ* bigQ = (BigQ*)start;
        bigQ->SortPhase ();
        bigQ->MergePhase ();
    }

    BigQ (Pipe& in, Pipe& out, OrderMaker& sortorder, int length);
    ~BigQ () {};

};

class RecordComparer {
private:
    OrderMaker* sortordering;
public:
    bool operator() (Record* leftRecord, Record* rightRecord);
    RecordComparer (OrderMaker* sort_order);
};

class Run {

private:

    Page pageRun;

public:

    int pageOffSetRun;
    int pageSizeRun;

    Record* headRecordInstance;
    OrderMaker* sortordering;
    File* fileRun;

    int getHead();

    Run (int length, int offset, File* runfileName, OrderMaker* sort_order);
    Run (File* runfileName, OrderMaker* sort_order);
    ~Run();

};


#endif