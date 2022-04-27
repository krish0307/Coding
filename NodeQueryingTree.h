#ifndef QUERY_TREE_NODE_H
#define QUERY_TREE_NODE_H

#define P_SIZE 100

#include "Schema.h"
#include "Record.h"
#include "Function.h"
#include "Comparison.h"
#include "RelOp.h"
#include "DBFile.h"
#include <vector>

extern struct FuncOperator *finalFunction;
extern struct NameList *attsToSelect;
enum QueryTypes {SELECTF, SELECTP, PROJECT, JOIN, SUM, GROUP_BY, DISTINCT, WRITE};

class NodeQueryingTree {
 public:
		SelectFile *fileSelect;
		SelectPipe *pipeSelect;
		Join *joinOp;
		GroupBy *groupByOp;
		Project *projectOp;
		Sum *sumOp;
		DuplicateRemoval *duplicateRemOp;
		WriteOut *writeOutOp;
		DBFile *dbFileInstance;
	    QueryTypes queryNodeType;
	    NodeQueryingTree *queryNodeParent;
	    NodeQueryingTree *queryNodeLeft;
	    NodeQueryingTree *queryNodeRight;
		int leftChildId;
		Pipe *leftInputPipe;
		int rightChildId;
		Pipe *rightInputPipe;
		int outputPipeId;
		Pipe *outputPipe;
	    NodeQueryingTree();
	 	~NodeQueryingTree();
		void PrintInOrder();
	  	void NodePrinting ();
		void CNFPrinting();
		void FuncPrint();
    	void printIDsFromType(QueryTypes type);
		void SetType(QueryTypes typeInfo);
		void Run();
		void WaitUntilDone();
	    string GetTypeName ();
	    QueryTypes GetType ();
		void GenerateSchema();
		void GenerateFunction();
		void GenerateOM(int numAtts, vector<int> whichAtts, vector<int> whichTypes);
		int numAttsIn;
		int numAttsOut;
		vector<int> aTK;
	  	int *attsToKeep;
	  	int numAttsToKeep;
	    AndList *andListCNF;
	    CNF *operationCNF;
	    Schema *schemaInstance;
	    OrderMaker *orderMakerInstance;
	    FuncOperator *funcOperatorIns;
		Function *funcInstance;
		string pathString;
		string lookupKey(string path);
	    vector<string> relations;
};

#endif
