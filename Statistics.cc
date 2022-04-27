#include "Statistics.h"
#include <sstream>

InfoRelation :: InfoRelation () : joinCheck(false){
}

InfoRelation :: InfoRelation (const InfoRelation& copyMe) : joinCheck(copyMe.joinCheck) , relInfoName(copyMe.relInfoName) , totalTuplesCount(copyMe.totalTuplesCount) {
    attributeMap.insert (copyMe.attributeMap.begin (), copyMe.attributeMap.end ());
}

InfoRelation :: InfoRelation (string name, int tuples) :  joinCheck(false) , relInfoName(name) , totalTuplesCount(tuples) {
}

bool InfoRelation :: isRelationPresent (string _relName) {
    return (relInfoName == _relName) ? true : relJoint.count(_relName);
}

InfoRelation &InfoRelation :: operator= (const InfoRelation& copyMe) {
    attributeMap.insert (copyMe.attributeMap.begin (), copyMe.attributeMap.end ());
    totalTuplesCount = copyMe.totalTuplesCount, relInfoName = copyMe.relInfoName;
    joinCheck = copyMe.joinCheck;
	return *this;
}

InfoAttribute :: InfoAttribute () {}

InfoAttribute :: InfoAttribute (string name, int num) : attributeName(name), uniqueTuples(num){
}

InfoAttribute :: InfoAttribute (const InfoAttribute& copyMe) : attributeName(copyMe.attributeName), uniqueTuples(copyMe.uniqueTuples) {
}

InfoAttribute &InfoAttribute :: operator= (const InfoAttribute& copyMe){
    uniqueTuples = copyMe.uniqueTuples, attributeName = copyMe.attributeName;
	return *this;
}

Statistics :: ~Statistics () {}

Statistics Statistics :: operator= (Statistics& copyMe) {
    relMap.insert (copyMe.relMap.begin (), copyMe.relMap.end ());
    return *this;
}

Statistics :: Statistics () {}

Statistics :: Statistics (Statistics& copyMe) {
	relMap.insert (copyMe.relMap.begin (), copyMe.relMap.end ());
}

double Statistics :: AndOperation (AndList* andList, char* relName[], int numJoin) {
	return (andList != NULL) ? (OrOperation (andList->left, relName, numJoin)) * (AndOperation (andList->rightAnd, relName, numJoin)) : 1.0;
}

double Statistics :: OrOperation (OrList* orList, char* relName[], int numJoin) {
	if (orList == NULL) {return 0.0;}
    int counter = 1;
    char* attributeName = orList->left->left->value;
	OrList* tmpOrderList = orList->rightOr;
    double leftVal = CompareOperation (orList->left, relName, numJoin);
	while (tmpOrderList) {
		if (!strcmp(tmpOrderList->left->left->value, attributeName)) {counter++;}
		tmpOrderList = tmpOrderList->rightOr;
	}
	return (counter > 1) ? (double) counter * leftVal : (double) (1.0 - (1.0 - leftVal)*(1.0 - (OrOperation (orList->rightOr, relName, numJoin))));
}

double Statistics :: CompareOperation (ComparisonOp* compOp, char* relName[], int numJoin) {
    double leftVal, rightVal;
    InfoRelation lRelInfo, rRelInfo;
	int code = compOp->code;
	int lRes = GetRelInfoOperation (compOp->left, relName, numJoin, lRelInfo);
	int rRes = GetRelInfoOperation (compOp->right, relName, numJoin, rRelInfo);
	if (compOp->right->code != NAME) {
        rightVal = -1.0;
	} else {
        if (rRes != -1) {
            string tmpString(compOp->right->value);
            rightVal = rRelInfo.attributeMap[tmpString].uniqueTuples;
        } else {
            rightVal = 1.0;
        }
	}
    if (compOp->left->code != NAME) {
        leftVal = -1.0;
    } else {
        if (lRes != -1) {
            string tmpString(compOp->left->value);
            leftVal = lRelInfo.attributeMap[tmpString].uniqueTuples;
        } else {
            leftVal = 1.0;
        }
    }
    if (code == EQUALS) {
        return (leftVal > rightVal) ? 1.0 / leftVal : 1.0 / rightVal;
    } else{
		return (code == LESS_THAN || code == GREATER_THAN) ? 1.0 / 3.0 : 0.0;
	}
}

void Statistics :: AddAtt(char* relName, char* attrName, int numDistincts) {
	string attributStr(attrName), relInfoStr(relName);
	InfoAttribute tmpAttributeInfo(attributStr, numDistincts);
	relMap[relInfoStr].attributeMap[attributStr] = tmpAttributeInfo;
}

int Statistics :: GetRelInfoOperation(Operand* operand, char* relName[], int numJoin, InfoRelation& relInfo) {
    if (operand == NULL || relName == NULL) {return -1;}
    string buffString(operand->value);
    for (auto eachReal : relMap) {
        if (eachReal.second.attributeMap.count(buffString)) {
            relInfo = eachReal.second;
            return 0;
        }
    }
    return -1;
}

void Statistics :: CopyRel (char* oldName, char* newName) {
	string latestStr(newName), oldStr(oldName);
    InfoAttrMap attributeMap;
	relMap[latestStr] = relMap[oldStr];
	relMap[latestStr].relInfoName = latestStr;
	for (auto eachAttr : relMap[latestStr].attributeMap) {
		string newAttrStr = latestStr + "." + eachAttr.first;
		InfoAttribute temp(eachAttr.second);
		temp.attributeName = newAttrStr;
		attributeMap[newAttrStr] = temp;
	}
	relMap[latestStr].attributeMap = attributeMap;
}

void Statistics :: Read (char* fromWhere) {
	int relInfoCounter, jointCounter, attributeNum, counterTuples, uniqueCounter,value=0;
	string tmpString;
	ifstream inSteam(fromWhere);
	relMap.clear ();
    string relInfoName, jointNameStr, attributeName;
	inSteam >> relInfoCounter;

	for (int i = 0; i < relInfoCounter; i++) {
		inSteam >> relInfoName;
		inSteam >> counterTuples;
		value++;
		InfoRelation tmpRelInfo(relInfoName, counterTuples);
		relMap[relInfoName] = tmpRelInfo;
		value=value+2;
		inSteam >> attributeNum;
		for (int j = 0; j < attributeNum; j++) {
		    inSteam >> tmpString;
			inSteam >> attributeName;
			inSteam >> uniqueCounter;
			value++;
			InfoAttribute tmpAttrInfo(attributeName, uniqueCounter);
			relMap[relInfoName].attributeMap[attributeName] = tmpAttrInfo;
			value--;
		}
	}
}

void Statistics :: AddRel (char *relName, int numTuples) {
    bool validate = false;
    string infoString(relName);
    InfoRelation tmpInfo(infoString, numTuples);
    validate = true;
    relMap[infoString] = tmpInfo;
}

void Statistics :: Apply (struct AndList* parseTree, char* relNames[], int numToJoin) {
    char* relNamesArr[100];
    int idx = 0, joiningNum = 0;
    bool validate;
	InfoRelation tmpInfo;
	while (idx < numToJoin) {
		string tmpString(relNames[idx]);
		if (relMap.count(tmpString)){
			tmpInfo = relMap[tmpString];
			relNamesArr[joiningNum++] = relNames[idx];
			validate = false;
			if (tmpInfo.joinCheck && tmpInfo.relJoint.size() <= numToJoin) {
			    for (int i = 0; i < numToJoin; i++) {
			        validate = true;
			        string str(relNames[i]);
			        if (tmpInfo.relJoint.count(str) && tmpInfo.relJoint[str] != tmpInfo.relJoint[tmpString]) {
			            validate = false;
			            return;
			        }
			    }
			}
		}
		idx++;
	}
	string startingRelName(relNamesArr[0]);
	InfoRelation firstInfo = relMap[startingRelName];
    firstInfo.totalTuplesCount = Estimate (parseTree, relNamesArr, joiningNum);
    firstInfo.joinCheck = true;
    relMap.erase (startingRelName);
    validate = true;
	for(int i = 1;i < joiningNum;i++){
		string tempStr(relNamesArr[i]);
		firstInfo.relJoint[tempStr] = tempStr;
		tmpInfo = relMap[tempStr];
        validate = false;
		relMap.erase (tempStr);
		firstInfo.attributeMap.insert (tmpInfo.attributeMap.begin (), tmpInfo.attributeMap.end ());
		validate = true;
	}
	relMap[startingRelName] = firstInfo;
}

void Statistics :: Write (char* toWhere) {
    ofstream outStream (toWhere);
    outStream << relMap.size() << endl;
    for (auto eachRel : relMap) {
        outStream << eachRel.second.relInfoName << " ";
        outStream << eachRel.second.totalTuplesCount << " ";
        outStream << eachRel.second.attributeMap.size () << endl;
        for (auto eachAttr : eachRel.second.attributeMap) {
            outStream << "# ";
            outStream << eachAttr.second.attributeName << endl;
            outStream << eachAttr.second.uniqueTuples << endl;
        }
    }
    outStream.close ();
}

double Statistics :: Estimate (struct AndList *parseTree, char **relNames, int numToJoin) {
    double multipliedValue = 1.0;
	for(int i = 0; i < numToJoin;i++) {
		string tmpString(relNames[i]);
		if (relMap.count(tmpString)) {
            multipliedValue *= (double) relMap[tmpString].totalTuplesCount;
        }
	}
	return (parseTree != NULL) ? (AndOperation (parseTree, relNames, numToJoin))*multipliedValue : multipliedValue ;
}
void Statistics::ParseRelation(struct Operand* op, string& relation) {

    string value(op->value);
    string reln;
    stringstream s;

    int i = 0;

    while (value[i] != '_') {

        if (value[i] == '.') {
            relation = s.str();
            return;
        }

        s << value[i];

        i++;

    }

    //relation = s.str();
    //reln = s.str();
    //relation = tables[rel];

}
