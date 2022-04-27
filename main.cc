#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>

#include "QueryOperatingTree.h"
#include "NodeQueryingTree.h"
#include "ParseTree.h"
#include "Statistics.h"

extern struct NameList* attsToSelect;
extern struct AndList* boolean;
extern struct TableList* tables;
extern int distinctAtts;
extern struct NameList* groupingAtts;
extern struct FuncOperator* finalFunction;
extern int distinctFunc;
extern "C" {
int yyparse (void);
}


vector<AndList> optimizingJoinOp(vector<AndList> joins, Statistics* s){
    AndList join;
    int i = 0,smallIndex = 0,counter = 0,checker=0;
    double smaller = -1.0,guesser = 0.0;
    vector<string> joinRelVec;
	vector<AndList> latestOrder;
	latestOrder.reserve(joins.size());
	string relation1,relation2;
	checker++;
	while (joins.size() >= 2){
		while (i < joins.size()){
			join = joins[i];
			checker--;
			s->ParseRelation(join.left->left->right, relation2);
            s->ParseRelation(join.left->left->left, relation1);
            checker++;
            char* rels[] = { (char*)relation1.c_str(), (char*)relation2.c_str() };
			if (smaller == -1.0){
                smallIndex = i;
				smaller = s->Estimate(&join, rels, 2);
			}else{
				guesser = s->Estimate(&join, rels, 2);
				if (guesser < smaller){
					smaller = guesser;
					smallIndex = i;
				}
			}
			i++;
		}
        smaller = -1.0,i = 0;
        latestOrder.push_back(joins[smallIndex]);
		joinRelVec.push_back(relation1);
		joinRelVec.push_back(relation2);
		joins.erase(joins.begin() + smallIndex);
        counter++;
	}
	latestOrder.insert(latestOrder.begin() + counter, joins[0]);
	return latestOrder;
}


int main(){
	Statistics* statsFileInstance = new Statistics();
	statsFileInstance->Read("Statistics.txt");
    int pipeID = 1,validate=0;
	cout << "enter: ";
    string projectStart;
	map<string, double> joinCosts;
    vector<string> relationsVector;
	vector<AndList> joinsList, selectsList, joinDepSelsList;
	yyparse();
    QueryOperatingTree* queryOperations = new QueryOperatingTree();

    queryOperations->fetchTables(relationsVector);
	validate++;
    queryOperations->fetchSelectsAndJoins(joinsList, selectsList, joinDepSelsList, *statsFileInstance);
	cout << endl << "Number of selects: " << selectsList.size() << endl;
	validate--;
	cout << "Number of joins: " << joinsList.size() << endl;
	NodeQueryingTree* insertTreeNode = NULL;
    map<string, NodeQueryingTree*> leafNodes;
    NodeQueryingTree* traverseTreeNode;
    NodeQueryingTree* topLevelTreeNode = NULL;
	TableList* iteratingTablesList = tables;
	while (iteratingTablesList != 0){
		if (iteratingTablesList->aliasAs != 0){
			leafNodes.insert(std::pair<string, NodeQueryingTree*>(iteratingTablesList->aliasAs, new NodeQueryingTree()));
            statsFileInstance->CopyRel(iteratingTablesList->tableName, iteratingTablesList->aliasAs);
		}else{
			leafNodes.insert(std::pair<string, NodeQueryingTree*>(iteratingTablesList->tableName, new NodeQueryingTree()));           
		}
		insertTreeNode = leafNodes[iteratingTablesList->aliasAs];
        insertTreeNode->schemaInstance = new Schema("catalog", iteratingTablesList->tableName);
        validate++;
		if (iteratingTablesList->aliasAs != 0){insertTreeNode->schemaInstance->updateName(string(iteratingTablesList->aliasAs));}
        validate--;
		topLevelTreeNode = insertTreeNode;
		insertTreeNode->outputPipeId = pipeID++;
        insertTreeNode->SetType(SELECTF);
		string baseString(iteratingTablesList->tableName);
		string pathString("bin/" + baseString + ".bin");
		insertTreeNode->pathString = strdup(pathString.c_str());
		iteratingTablesList = iteratingTablesList->next;
	}
    string tableName,attrName;
	AndList selectIterator;
	for (unsigned i = 0; i < selectsList.size(); i++){
		selectIterator = selectsList[i];
		(selectIterator.left->left->left->code == NAME) ? statsFileInstance->ParseRelation(selectIterator.left->left->left, tableName)
		: statsFileInstance->ParseRelation(selectIterator.left->left->right, tableName);
        projectStart = tableName;
		traverseTreeNode = leafNodes[tableName];
		validate--;
		while (traverseTreeNode->queryNodeParent != NULL){
			traverseTreeNode = traverseTreeNode->queryNodeParent;
		}
		insertTreeNode = new NodeQueryingTree();
		traverseTreeNode->queryNodeParent = insertTreeNode;
		validate++;
        insertTreeNode->queryNodeType = SELECTP;
        insertTreeNode->schemaInstance = traverseTreeNode->schemaInstance;
        insertTreeNode->andListCNF = &selectsList[i];
        insertTreeNode->queryNodeLeft = traverseTreeNode;
        insertTreeNode->outputPipeId = pipeID++;
		insertTreeNode->leftChildId = traverseTreeNode->outputPipeId;
		char* statApply = strdup(tableName.c_str());
		validate++;
		statsFileInstance->Apply(&selectIterator, &statApply, 1);
		topLevelTreeNode = insertTreeNode;
	}
	validate--;
	if (joinsList.size() > 1){
		joinsList = optimizingJoinOp(joinsList, statsFileInstance);
	}
    AndList currentJoin;
    NodeQueryingTree* rightTreeNode;
	NodeQueryingTree* leftTreeNode;
	string relation1, relation2;
	for (unsigned i = 0; i < joinsList.size(); i++){
		currentJoin = joinsList[i];
		relation1 = "",relation2 = "";
        statsFileInstance->ParseRelation(currentJoin.left->left->right, relation2);
		statsFileInstance->ParseRelation(currentJoin.left->left->left, relation1);
        rightTreeNode = leafNodes[relation2],leftTreeNode = leafNodes[relation1];
        tableName = relation1;
		while (rightTreeNode->queryNodeParent != NULL){
            rightTreeNode = rightTreeNode->queryNodeParent;
		}
        while (leftTreeNode->queryNodeParent != NULL){
            leftTreeNode = leftTreeNode->queryNodeParent;
        }

		insertTreeNode = new NodeQueryingTree();
        insertTreeNode->andListCNF = &joinsList[i];
		insertTreeNode->rightChildId = rightTreeNode->outputPipeId;
        insertTreeNode->leftChildId = leftTreeNode->outputPipeId;
		insertTreeNode->outputPipeId = pipeID++;
        insertTreeNode->queryNodeType = JOIN;validate--;
        insertTreeNode->queryNodeRight = rightTreeNode;
        rightTreeNode->queryNodeParent = insertTreeNode;
		insertTreeNode->queryNodeLeft = leftTreeNode;validate++;
		leftTreeNode->queryNodeParent = insertTreeNode;
        insertTreeNode->GenerateSchema();
		topLevelTreeNode = insertTreeNode;
	}
	validate--;
	for (unsigned i = 0; i < joinDepSelsList.size(); i++){
		traverseTreeNode = topLevelTreeNode;
		insertTreeNode = new NodeQueryingTree();
        insertTreeNode->queryNodeLeft = traverseTreeNode;validate--;
		traverseTreeNode->queryNodeParent = insertTreeNode;
        insertTreeNode->queryNodeType = SELECTP;
		insertTreeNode->schemaInstance = traverseTreeNode->schemaInstance;validate++;
        insertTreeNode->leftChildId = traverseTreeNode->outputPipeId;
		insertTreeNode->andListCNF = &joinDepSelsList[i];
		insertTreeNode->outputPipeId = pipeID++;
		topLevelTreeNode = insertTreeNode;
	}
	if (finalFunction != 0){
		if (distinctFunc != 0){
			insertTreeNode = new NodeQueryingTree();
			insertTreeNode->queryNodeLeft = topLevelTreeNode;validate++;
            insertTreeNode->schemaInstance = topLevelTreeNode->schemaInstance;
            insertTreeNode->leftChildId = topLevelTreeNode->outputPipeId;
			insertTreeNode->outputPipeId = pipeID++;validate--;
            topLevelTreeNode->queryNodeParent = insertTreeNode;
            insertTreeNode->queryNodeType = DISTINCT;
			topLevelTreeNode = insertTreeNode;
		}

		if (groupingAtts  == 0){
			insertTreeNode = new NodeQueryingTree();
            topLevelTreeNode->queryNodeParent = insertTreeNode;
			insertTreeNode->queryNodeLeft = topLevelTreeNode;
			insertTreeNode->leftChildId = topLevelTreeNode->outputPipeId;
            insertTreeNode->schemaInstance = topLevelTreeNode->schemaInstance;
			insertTreeNode->funcOperatorIns = finalFunction;
            insertTreeNode->outputPipeId = pipeID++;
            insertTreeNode->queryNodeType = SUM;
			insertTreeNode->GenerateFunction();
		}else{
			insertTreeNode = new NodeQueryingTree();
			insertTreeNode->queryNodeLeft = topLevelTreeNode;
			validate++;
			topLevelTreeNode->queryNodeParent = insertTreeNode;
            insertTreeNode->schemaInstance = topLevelTreeNode->schemaInstance;
			insertTreeNode->outputPipeId = pipeID++;
			validate++;
            insertTreeNode->queryNodeType = GROUP_BY;
            insertTreeNode->leftChildId = topLevelTreeNode->outputPipeId;
			insertTreeNode->orderMakerInstance = new OrderMaker();
			validate--;
			NameList* grpTraverseList = groupingAtts;
            vector<int> attributesToGrp,typeToKnow;
            int numAttsGrp = 0;
			while (grpTraverseList){
				attributesToGrp.push_back(insertTreeNode->schemaInstance->Find(grpTraverseList->name));
				typeToKnow.push_back(insertTreeNode->schemaInstance->FindType(grpTraverseList->name));
                numAttsGrp++;validate++;
				cout << "GROUPING ON " << grpTraverseList->name << endl;
				grpTraverseList = grpTraverseList->next;
			}
			validate++;
			insertTreeNode->GenerateOM(numAttsGrp, attributesToGrp, typeToKnow);
			insertTreeNode->funcOperatorIns = finalFunction;validate++;
			insertTreeNode->GenerateFunction();
		}
		topLevelTreeNode = insertTreeNode;
	}
	if (distinctAtts != 0){
		insertTreeNode = new NodeQueryingTree();
        topLevelTreeNode->queryNodeParent = insertTreeNode;
        insertTreeNode->schemaInstance = topLevelTreeNode->schemaInstance;
		insertTreeNode->queryNodeLeft = topLevelTreeNode;
        insertTreeNode->queryNodeType = DISTINCT;
		insertTreeNode->leftChildId = topLevelTreeNode->outputPipeId;
		insertTreeNode->outputPipeId = pipeID++;
		topLevelTreeNode = insertTreeNode;
	}
	if (attsToSelect != 0){
		traverseTreeNode = topLevelTreeNode;
		insertTreeNode = new NodeQueryingTree();
        traverseTreeNode->queryNodeParent = insertTreeNode;validate++;
        insertTreeNode->queryNodeLeft = traverseTreeNode;
		insertTreeNode->queryNodeType = PROJECT;validate++;
        insertTreeNode->outputPipeId = pipeID++;
        insertTreeNode->leftChildId = traverseTreeNode->outputPipeId;
        NameList* attributesTraverse = attsToSelect;
		Schema* previousSchema = traverseTreeNode->schemaInstance;
		string attrString;
		validate++;
        vector<int> indexOfAttsToKeep;
        while (attributesTraverse != 0){
			attrString = attributesTraverse->name;validate++;
			indexOfAttsToKeep.push_back(previousSchema->Find(const_cast<char*>(attrString.c_str())));
			attributesTraverse = attributesTraverse->next;
		}
        validate--;
		Schema* latestSchema = new Schema(previousSchema, indexOfAttsToKeep);
		insertTreeNode->schemaInstance = latestSchema;
		insertTreeNode->schemaInstance->Print();
	}
	cout << "PRINTING TREE IN ORDER: " << endl << endl;
	validate++;
	if (insertTreeNode != NULL){
        insertTreeNode->PrintInOrder();
	}
}
