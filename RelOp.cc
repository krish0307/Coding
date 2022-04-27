#include "RelOp.h"
using namespace std;
int bufferLimit = 100;

void RelOp :: Use_n_Pages (int n) {
	runLen = n;
}

void *_StartOp (void *arg) {
    ((RelOp *)arg)->Begin ();
}

void RelOp :: WaitUntilDone () {
	pthread_join (t, NULL);
}


void DuplicateRemoval :: Run (Pipe &inPipe, Pipe &outPipe,Schema &mySchema) {
    pipeIn = &inPipe, pipeOut = &outPipe, schema = &mySchema;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void SelectPipe :: Run (Pipe &inPipe,Pipe &outPipe,CNF &selOp,Record &literal) {
    pipeIn = &inPipe, pipeOut = &outPipe, cnf = &selOp , rec = &literal;
    pthread_create(&t, NULL, _StartOp, (void *) this);
}

void Project :: Run ( Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    pipeIn = &inPipe, pipeOut = &outPipe, attsToKeep = keepMe, numAttsIn = numAttsInput, numAttsOut = numAttsOutput;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void SelectFile :: Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	file = &inFile, out = &outPipe, cnf = &selOp, rec = &literal;
	pthread_create (&t, NULL, _StartOp, (void *) this);
}

void Join :: Run (Pipe &inL, Pipe &inR, Pipe &outPipe, CNF &selOp, Record &literal) {
    rec = &literal, inPipeLeft = &inL,out = &outPipe, cnf = &selOp,inPipeRight = &inR;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void GroupBy :: Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,Function &computeMe) {
    pipeIn = &inPipe, pipeOut = &outPipe, orderMaker = &groupAtts, compute = &computeMe;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void Sum :: Run (Pipe &inPipe,Pipe &outPipe,Function &computeMe) {
    pipeIn = &inPipe, pipeOut = &outPipe, compute = &computeMe;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void WriteOut :: Run (Pipe &inPipe,FILE *outFile,Schema &mySchema) {
    pipeIn = &inPipe, file = outFile, schema = &mySchema;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void Project :: Begin () {
    Record *tmpRecord = new Record ();
    int counter = 0;
    while (pipeIn->Remove (tmpRecord)) {
        tmpRecord->Project (attsToKeep, numAttsOut, numAttsIn);
        pipeOut->Insert (tmpRecord);
        ++counter;
    }
    pipeOut->ShutDown ();

    delete tmpRecord;
}

void SelectFile :: Begin () {
	Record *tmpRecord = new Record ();
	file->MoveFirst();
	while (file->GetNext (*tmpRecord, *cnf, *rec)) {
		out->Insert (tmpRecord);
	}
	out->ShutDown ();
	delete tmpRecord;
}

void Join :: Begin () {
    int value = 0,lAtts, rAtts, totalAtts;
	ComparisonEngine cmprEngine;
	Record *rRec = new Record ();
	Record *lRec = new Record ();
    OrderMaker leftOrder, rightOrder;
	int *tmpAttr;
    cnf->GetSortOrders(leftOrder, rightOrder);
	if (leftOrder.numAtts > 0 && rightOrder.numAtts > 0) {
		bool validate = false;
		Pipe leftPipe (bufferLimit), rightPipe (bufferLimit);
		BigQ leftBigQ (*inPipeLeft, leftPipe, leftOrder, runLen), rightBigQ (*inPipeRight, rightPipe, rightOrder, runLen);
		(!leftPipe.Remove (lRec)) ? validate = true : lAtts = lRec->GetLength ();
		if (!validate && !rightPipe.Remove (rRec)) {
			validate = true;
		} else {
            int check = 0;
			rAtts = rRec->GetLength ();
	    	totalAtts = lAtts + rAtts;
			tmpAttr = new int[totalAtts];
			while(check < lAtts || check < rAtts){
			    if(check < lAtts){tmpAttr[check] = check;}
			    if(check < rAtts){tmpAttr[lAtts + check] = check;}
			    check++;
			}
		}

		while (!validate) {
			while (cmprEngine.Compare (lRec, &leftOrder, rRec, &rightOrder) > 0) {
				if (!rightPipe.Remove (rRec)) {
                    value++;
					validate = true;
					break;
				}
			}
			while (!validate && cmprEngine.Compare (lRec, &leftOrder, rRec, &rightOrder) < 0) {
				if (!leftPipe.Remove (lRec)) {
				    value++;
					validate = true;
					break;
				}
			}
            value--;
			while (!validate && cmprEngine.Compare (lRec, &leftOrder, rRec, &rightOrder) == 0) {
				Record *tempRec = new Record ();
				tempRec->MergeRecords (lRec,rRec,lAtts,rAtts,tmpAttr,totalAtts,lAtts);
                value++;
				out->Insert (tempRec);
				if (!rightPipe.Remove (rRec)) {
				    value++;
					validate = true;
					break;
				}
			}
		}
		while (rightPipe.Remove (rRec));
		while (leftPipe.Remove (lRec));
	} else {
        DBHeapFile heapFile;
		char fileName[100];
        bool validate = false;
		sprintf (fileName, "temp.tmp");
		heapFile.Create (fileName);
		(!inPipeLeft->Remove (lRec)) ? validate = true : lAtts = lRec->GetLength ();
		if (!inPipeRight->Remove (rRec)) {
			validate = true;
		} else {
            int check = 0;
			rAtts = rRec->GetLength ();
			totalAtts = lAtts + rAtts;
			tmpAttr = new int[totalAtts];
            while(check < lAtts || check < rAtts){
                if(check < lAtts){tmpAttr[check] = check;}
                if(check < rAtts){tmpAttr[lAtts + check] = check;}
                check++;
            }
		}
		if (!validate) {
            heapFile.Add (*lRec);
		    while(inPipeLeft->Remove (lRec)){
                heapFile.Add (*lRec);
			}

		    heapFile.MoveFirst ();
		    Record *tmpRecord = new Record ();
		    while (heapFile.GetNext (*lRec)) {
		        if (cmprEngine.Compare (lRec, rRec, rec, cnf)) {
		            value++;
		            tmpRecord->MergeRecords (lRec,rRec,lAtts,rAtts,tmpAttr,totalAtts,lAtts);
		            out->Insert (tmpRecord);
		        }
		    }
		    delete tmpRecord;
		    value++;
	        while (inPipeRight->Remove (rRec)){
                heapFile.MoveFirst ();
                Record *tmpRecord = new Record ();
                while (heapFile.GetNext (*lRec)) {
                   if (cmprEngine.Compare (lRec, rRec, rec, cnf)) {
                       value++;
                       tmpRecord->MergeRecords (lRec,rRec,lAtts,rAtts,tmpAttr,totalAtts,lAtts);
                       out->Insert (tmpRecord);
                   }
                }
                delete tmpRecord;
			}

		}
		heapFile.Close ();
		remove ("temp.tmp");
	}
	out->ShutDown ();
    delete rRec, delete lRec, delete tmpAttr;
}

void SelectPipe :: Begin () {
    Record *tmpRecord = new Record ();
    ComparisonEngine cmprEngine;
    int counter = 0;
    while (pipeIn->Remove (tmpRecord)) {
        if (cmprEngine.Compare (tmpRecord, rec, cnf)) {
            pipeOut->Insert (tmpRecord);
            ++counter;
        }
    }
    pipeOut->ShutDown ();
    delete tmpRecord;
}

void DuplicateRemoval :: Begin () {
    Record *previousRecord = new Record ();
    OrderMaker sortOrderMaker (schema);
    Pipe tmpPipe (bufferLimit);
	BigQ bigq (*pipeIn, tmpPipe, sortOrderMaker, runLen);
    ComparisonEngine cmprEngine;
	tmpPipe.Remove (previousRecord);
    Record *currentRecord = new Record ();
	while (tmpPipe.Remove (currentRecord)) {
		if (cmprEngine.Compare (previousRecord, currentRecord, &sortOrderMaker)) {
			pipeOut->Insert (previousRecord);
			previousRecord->Copy (currentRecord);
		}
	}
	bool validate = false;
	if (currentRecord->bits != NULL && !cmprEngine.Compare (previousRecord,currentRecord, &sortOrderMaker)) {
		pipeOut->Insert (previousRecord);
        validate = true;
		previousRecord->Copy (currentRecord);
	}
	pipeOut->ShutDown ();
    delete previousRecord,delete currentRecord;
}

void Sum :: Begin () {
	Attribute tmpAttributes;
	Record *tempRec = new Record ();
    tmpAttributes.name = "SUM";
	double sum = 0.0, recDouble;
    int sumInt = 0, recInt, valueVal = 0;
	if (!pipeIn->Remove (tempRec)) {
		pipeOut->ShutDown ();
		return;
	}
	tmpAttributes.myType = compute->Apply (*tempRec, recInt, recDouble);
	(tmpAttributes.myType == Int) ? sumInt += recInt : sum += recDouble;
	while (pipeIn->Remove (tempRec)) {
		compute->Apply (*tempRec, recInt, recDouble);
		(tmpAttributes.myType == Int) ? sumInt += recInt : sum += recDouble;
	}
    Schema sumSchema (NULL, 1, &tmpAttributes);
    stringstream stringStreamSS;
	(tmpAttributes.myType != Int) ?  stringStreamSS << sum << '|' : stringStreamSS << sumInt << '|';
    tempRec->ComposeRecord (&sumSchema, stringStreamSS.str ().c_str ());
    valueVal = sumInt;
	pipeOut->Insert (tempRec);
	pipeOut->ShutDown ();
}

void WriteOut :: Begin () {
    Record *tempRecord = new Record ();
    while (pipeIn->Remove (tempRecord)) {
        tempRecord->WriteToFile (file, schema);
    }
    fclose (file);
    delete tempRecord;
}

void GroupBy :: Begin () {
    int *attributes = orderMaker->whichAtts;
    double sumDouble = 0.0, recDouble;
    int validate = 0,sumInt = 0, recInt, numOfAtts = orderMaker->numAtts;
	Type tmpType;
	Pipe tmpPipe (bufferLimit);
    int *keepAtts = new int[numOfAtts + 1];
    char *sumString = new char[bufferLimit];
    keepAtts[0] = 0;
	BigQ bigq (*pipeIn, tmpPipe, *orderMaker, runLen);
    for (int i = 0; i < numOfAtts; i++) {
        keepAtts[i + 1] = attributes[i];
    }
    Record *previousRecord = new Record ();
	if (tmpPipe.Remove (previousRecord)) {
		tmpType = compute->Apply (*previousRecord, recInt, recDouble);
		(tmpType == Int) ? sumInt += recInt : sumDouble += recDouble;
	} else {
		pipeOut->ShutDown();
		delete sumString, delete previousRecord;
		exit (-1);
	}
    Attribute tmpAttributes;
	tmpAttributes.myType = tmpType;
	tmpAttributes.name = "SUM";
    Record *currentRecord = new Record ();
    Record *tmpRecord = new Record ();
    Record *sumRecord = new Record ();
    Schema *sumSchema = new Schema (NULL, 1, &tmpAttributes);
	while (tmpPipe.Remove (currentRecord)) {
        ComparisonEngine cmprEngine;
		if (cmprEngine.Compare (previousRecord, currentRecord, orderMaker) != 0) {
			(tmpType == Int) ? sprintf (sumString, "%d|", sumInt) : sprintf (sumString, "%f|", sumDouble);
			sumRecord->ComposeRecord (sumSchema, sumString);
            validate = sumInt;
			tmpRecord->MergeRecords (sumRecord, previousRecord, 1, previousRecord->GetLength (), keepAtts, numOfAtts + 1, 1);
            sumDouble = 0.0;
			sumInt = 0;
			pipeOut->Insert (tmpRecord);
			compute->Apply (*currentRecord, recInt, recDouble);
			(tmpType == Int) ? sumInt += recInt : sumDouble += recDouble;
			previousRecord->Consume (currentRecord);
		} else {
			compute->Apply (*currentRecord, recInt, recDouble);
			(tmpType == Int) ? sumInt += recInt : sumDouble += recDouble;
		}
	}
	(tmpType == Int) ? sprintf (sumString, "%d|", sumInt) :sprintf (sumString, "%f|", sumDouble);
	sumRecord->ComposeRecord (sumSchema, sumString);
    validate++;
	tmpRecord->MergeRecords (sumRecord, previousRecord, 1, previousRecord->GetLength (), keepAtts, numOfAtts + 1, 1);
	pipeOut->Insert (tmpRecord);
    validate = sumInt;
	pipeOut->ShutDown ();
	delete sumString, delete sumSchema, delete previousRecord, delete currentRecord, delete sumRecord, delete tmpRecord;
}

