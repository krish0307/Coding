#ifndef STATISTICS_H
#define STATISTICS_H
#include "ParseTree.h"
#include <map>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <fstream>
#include <iostream>

using namespace std;
class InfoRelation;
typedef map<string, InfoRelation> InfoRelationMap;
class InfoAttribute;
typedef map<string, InfoAttribute> InfoAttrMap;

class InfoAttribute {
public:
    int uniqueTuples;
	string attributeName;

    InfoAttribute (string name, int num);
	InfoAttribute ();
    InfoAttribute &operator= (const InfoAttribute &copyMe);
	InfoAttribute (const InfoAttribute &copyMe);
};

class InfoRelation {
public:
    bool joinCheck;
	double totalTuplesCount;
    InfoAttrMap attributeMap;
	string relInfoName;
	map<string, string> relJoint;
    InfoRelation (const InfoRelation &copyMe);
	InfoRelation ();
	InfoRelation &operator= (const InfoRelation &copyMe);
	bool isRelationPresent (string _relName);
    InfoRelation (string name, int tuples);
};

class Statistics {
private:
    int GetRelInfoOperation (Operand *operand, char *relName[], int numJoin, InfoRelation &relInfo);
    double CompareOperation (ComparisonOp *comOp, char *relName[], int numJoin);
	double AndOperation (AndList *andList, char *relName[], int numJoin);
    double OrOperation (OrList *orList, char *relName[], int numJoin);
public:
    ~Statistics();
	InfoRelationMap relMap;
    Statistics operator= (Statistics &copyMe);
	Statistics();
	Statistics(Statistics &copyMe);
    void Write(char *toWhere);
    void CopyRel(char *oldName, char *newName);
    void AddRel(char *relName, int numTuples);
    void Read(char *fromWhere);
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
	void AddAtt(char *relName, char *attrName, int numDistincts);
	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
    void ParseRelation(struct Operand* op, string& relation);

};

#endif
