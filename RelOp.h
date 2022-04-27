#ifndef REL_OP_H
#define REL_OP_H

#include <iostream>
#include <vector>
#include <cstring>
#include <sstream>
#include <pthread.h>

#include "Pipe.h"
#include "DBFile.h"
#include "Schema.h"
#include "Record.h"
#include "Function.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class RelOp {
protected:
    pthread_t t;
	int runLen;
public:
	virtual void WaitUntilDone ();
	virtual void Begin () = 0;
    virtual void Use_n_Pages (int n);
};

class SelectPipe : public RelOp {
private:
    CNF *cnf;
	Record *rec;
    Pipe *pipeIn, *pipeOut;
public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void Begin ();
    SelectPipe () {};
    ~SelectPipe () {};
};

class SelectFile : public RelOp {
private:
    CNF *cnf;
	DBFile *file;
	Record *rec;
    Pipe *out;
public:
    void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
    SelectFile () {};
    void Begin ();
    ~SelectFile () {};
};

class Project : public RelOp {
private:
    int numAttsIn, numAttsOut;
	Pipe *pipeIn, *pipeOut;
	int *attsToKeep;
public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
    Project () {};
    ~Project () {};
	void Begin ();
};

class Join : public RelOp {
private:
    CNF *cnf;
    Record *rec;
	Pipe *inPipeLeft, *inPipeRight, *out;
public:
    void Begin ();
	~Join () {};
    Join () {};
	void Run (Pipe &inLeft, Pipe &inRight, Pipe &outPipe, CNF &selOp, Record &literal);
};

class DuplicateRemoval : public RelOp {
private:
    Schema *schema;
	Pipe *pipeIn, *pipeOut;
public:
    void Begin ();
    ~DuplicateRemoval () {};
	DuplicateRemoval () {};
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
};

class Sum : public RelOp {
private:
	Pipe *pipeIn, *pipeOut;
	Function *compute;
public:
    void Begin ();
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
    ~Sum () {};
    Sum () {};
};

class GroupBy : public RelOp {
private:
	OrderMaker *orderMaker;
    Pipe *pipeIn, *pipeOut;
	Function *compute;
public:
    void Begin ();
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
    GroupBy () {};
    ~GroupBy () {};
};

class WriteOut : public RelOp {
private:
    FILE *file;
	Pipe *pipeIn;
	Schema *schema;
public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
    ~WriteOut () {};
    WriteOut () {};
	void Begin ();
};

#endif