#include "Schema.h"
#include "Function.h"
#include "Comparison.h"
#include "NodeQueryingTree.h"
#include <iostream>
#include <vector>

using namespace std;

NodeQueryingTree::NodeQueryingTree() : leftChildId(0),rightChildId(0),outputPipeId(0),queryNodeParent(NULL), queryNodeLeft(NULL), queryNodeRight(NULL), attsToKeep(NULL), numAttsToKeep(NULL),andListCNF(NULL), operationCNF(NULL), schemaInstance(NULL), orderMakerInstance(NULL), funcOperatorIns(NULL), funcInstance(NULL), fileSelect(NULL), pipeSelect(NULL), joinOp(NULL), groupByOp(NULL), projectOp(NULL), sumOp(NULL), duplicateRemOp(NULL), writeOutOp(NULL), dbFileInstance(NULL){
}

void NodeQueryingTree::SetType(QueryTypes typeInfo){
    queryNodeType = typeInfo;
}

NodeQueryingTree::~NodeQueryingTree(){
}

QueryTypes NodeQueryingTree::GetType(){
    return queryNodeType;
}

void NodeQueryingTree::WaitUntilDone(){
    if(queryNodeType == SELECTF){
        fileSelect->WaitUntilDone();
    }
    if(queryNodeType == SELECTP) {
        pipeSelect->WaitUntilDone();
    }
}

std::string NodeQueryingTree::GetTypeName(){
    if(queryNodeType == SELECTF){
        return "SELECT FILE";
    }else if(queryNodeType == SELECTP){
        return "SELECT PIPE";
    }else if(queryNodeType == JOIN){
        return "JOIN";
    }else if(queryNodeType == GROUP_BY){
        return "GROUP BY";
    }else if(queryNodeType == DISTINCT){
        return "DISTINCT";
    }else if(queryNodeType == SUM){
        return "SUM";
    }else if(queryNodeType == PROJECT){
        return "PROJECT";
    }else{
        return "WRITE";
    }
}

void NodeQueryingTree::PrintInOrder(){
    if(queryNodeLeft != NULL){queryNodeLeft->PrintInOrder();}
    if(queryNodeRight != NULL){queryNodeRight->PrintInOrder();}
    NodePrinting();
}

void NodeQueryingTree::printIDsFromType(QueryTypes queryType){
    if(queryType == SELECTP || queryType == SELECTF || queryType == PROJECT){
        clog << "Input Pipe " << leftChildId << endl;
    }else if(queryType == SUM || queryType == DISTINCT || queryType == GROUP_BY){
        clog << "Left Input Pipe " << leftChildId << endl;
    }
    clog << "Output Pipe " << outputPipeId << endl;
    clog << "Output Schema: " << endl;
    schemaInstance->Print();
}


void NodeQueryingTree::NodePrinting(){
    clog << " *********** " << endl;
    clog << GetTypeName() << " operation" << endl;

    if(queryNodeType == SELECTF){
        printIDsFromType(SELECTF);
        CNFPrinting();
    }else if(queryNodeType == SELECTP){
        printIDsFromType(SELECTP);
        clog << "SELECTION CNF :" << endl;
        CNFPrinting();
    }else if(queryNodeType == SUM){
        printIDsFromType(SUM);
        clog << endl << "FUNCTION: " << endl;
        funcInstance->Print(finalFunction, *schemaInstance);
    }else if(queryNodeType == PROJECT){
        printIDsFromType(PROJECT);
        clog << endl << "************" << endl;
    }else if(queryNodeType == DISTINCT){
        printIDsFromType(DISTINCT);
        clog << endl << "FUNCTION: " << endl;
        funcInstance->Print(finalFunction, *schemaInstance);
    }else if(queryNodeType == JOIN){
        clog << "Left Input Pipe " << leftChildId << endl;
        clog << "Right Input Pipe " << rightChildId << endl;
        int validatingCheck = 0;
        clog << "Output Pipe " << outputPipeId << endl;
        clog << "Output Schema: " << endl;
        validatingCheck = 1;
        schemaInstance->Print();
        clog << endl << "CNF: " << endl;
        CNFPrinting();
    }else if(queryNodeType == GROUP_BY){
        printIDsFromType(GROUP_BY);
        clog << endl << "GROUPING ON " << endl;
        orderMakerInstance->Print();
        clog << endl << "FUNCTION " << endl;
        funcInstance->Print(finalFunction, *schemaInstance);
    }else{
        clog << "Left Input Pipe " << leftChildId << endl;
        clog << "Output File " << pathString << endl;
    }
}

void NodeQueryingTree::CNFPrinting(){
    int validatingCheck = 0;
    if (andListCNF){
        struct ComparisonOp *curOp;
        struct OrList *curOr;
        struct AndList *curAnd = andListCNF;
        while (curAnd){
            curOr = curAnd->left;
            if (curAnd->left){clog << "(";}
            validatingCheck++;
            while (curOr){
                curOp = curOr->left;
                validatingCheck--;
                if (curOp){
                    if (curOp->left){clog << curOp->left->value;}
                    validatingCheck++;
                    if(curOp->code == 5){
                        clog << " < ";
                    }else if(curOp->code == 6){
                        clog << " > ";
                    }else if(curOp->code == 7){
                        clog << " = ";
                    }
                    if (curOp->right){clog << curOp->right->value;}
                }
                validatingCheck--;
                if (curOr->rightOr){clog << " OR ";}
                curOr = curOr->rightOr;
                validatingCheck++;
            }
            if (curAnd->left){clog << ")";}
            if (curAnd->rightAnd) {clog << " AND ";}
            validatingCheck--;
            curAnd = curAnd->rightAnd;
        }
    }
    validatingCheck=0;
    clog << endl;
}

void NodeQueryingTree::GenerateFunction(){
    funcInstance = new Function();
    funcInstance->GrowFromParseTree(funcOperatorIns, *schemaInstance);
}

void NodeQueryingTree::GenerateOM(int numAtts, vector<int> attsList, vector<int> typesList){
    orderMakerInstance = new OrderMaker();
    int info=0;
    orderMakerInstance->numAtts = numAtts;
    while(info < attsList.size()){
        orderMakerInstance->whichAtts[info] = attsList[info];
        orderMakerInstance->whichTypes[info] = (Type)typesList[info];
        info++;
    }
}

void NodeQueryingTree::GenerateSchema(){
    schemaInstance = new Schema(queryNodeLeft->schemaInstance, queryNodeRight->schemaInstance);
}
