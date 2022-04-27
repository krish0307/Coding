//
// Created by Gowtham Avula on 4/22/20.
//

#include "QueryOperatingTree.h"
#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>

#include "NodeQueryingTree.h"
#include "ParseTree.h"
#include "Statistics.h"

QueryOperatingTree ::QueryOperatingTree() {}

extern "C" {
int yyparse (void);   // defined in y.tab.c
}

extern struct FuncOperator* finalFunction;
extern struct TableList* tables;
extern struct AndList* boolean;
extern struct NameList* groupingAtts;
extern struct NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

void QueryOperatingTree :: fetchTables(vector<string>& relations){
    TableList* tableList = tables;
    while (tableList){
        (tableList->aliasAs) ? relations.push_back(tableList->aliasAs) : relations.push_back(tableList->tableName);
        tableList = tableList->next;
    }
}

void QueryOperatingTree ::fetchSelectsAndJoins(vector <AndList> &joins, vector <AndList> &selects, vector <AndList> &joinDepSel, Statistics s){
    struct OrList* presentOrList;
    int validate = 0;
    while (boolean != 0){
        presentOrList = boolean->left;
        validate++;
        if (presentOrList && presentOrList->left->code == EQUALS && presentOrList->left->right->code == NAME && presentOrList->left->left->code == NAME){
            validate--;
            AndList latestAndList = *boolean;
            latestAndList.rightAnd = 0;
            validate++;
            joins.push_back(latestAndList);
        }else{
            validate++;
            presentOrList = boolean->left;
            if (presentOrList->left == 0){
                validate = 0;
                AndList latestAnd = *boolean;
                latestAnd.rightAnd = 0;
                validate++;
                selects.push_back(latestAnd);
            }else{
                vector<string> includedTables;
                validate--;
                while (presentOrList != 0){
                    Operand* operandOp = presentOrList->left->left;
                    string relInfo;
                    if (operandOp->code != NAME){
                        operandOp = presentOrList->left->right;
                    }
                    s.ParseRelation(operandOp, relInfo);
                    validate++;
                    if (includedTables.size() == 0 || relInfo.compare(includedTables[0]) != 0){
                        includedTables.push_back(relInfo);
                    }
                    validate--;
                    presentOrList = presentOrList->rightOr;
                }
                AndList latestAnd = *boolean;
                validate++;
                latestAnd.rightAnd = 0;
                (includedTables.size() > 1) ? joinDepSel.push_back(latestAnd) : selects.push_back(latestAnd);
            }
        }
        validate--;
        boolean = boolean->rightAnd;
    }
}