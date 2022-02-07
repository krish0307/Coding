#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <stdlib.h>
#include <iostream>
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
    this->f = new File();
    this->present_page = new Page(); 
}

DBFile::~DBFile () {
    delete f;
    delete present_page;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    this->f->Open(0, (char *) f_path);
    return 1;

}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE *schemaFile = fopen((char*) loadpath, "r");
    int counter_pages=0;

    if(schemaFile){
        Record r;
        Page current_page;
    
        while(r.SuckNextRecord(&f_schema, schemaFile)==1){
            int succ = current_page.Append(&r);
            if(succ==0){
                this->f->AddPage(&current_page, counter_pages++);
                current_page.EmptyItOut();
                current_page.Append(&r);
            }
        }
        this->f->AddPage(&current_page, counter_pages);
    }
    else{
        cout << "Error couldnt load the schema file %s", loadpath;
    }
    fclose(schemaFile);
}

int DBFile::Open (const char *f_path) {
    this->f->Open(1, const_cast<char *> (f_path) );
    return 1;
}

void DBFile::MoveFirst () {
    this->f->GetPage(this->present_page, 0);
}


int DBFile::Close () { 
    return this->f->Close();
}

void DBFile::Add (Record &rec) {
    if(this->present_page->Append(&rec)==0){
        this->f->AddPage(this->present_page, this->present_page_index);
        this->present_page_index++;
        this->present_page->EmptyItOut();
        this->present_page->Append(&rec);
    }
}

int DBFile::GetNext (Record &fetchme) {
    int resp_get = this->present_page->GetFirst(&fetchme);
    if(resp_get==0){
        if(++this->present_page_index < this->f->GetLength() - 1){
            this->f->GetPage(this->present_page, this->present_page_index );
            return this->present_page->GetFirst(&fetchme);
        }
        else{
            return 0;
        }
    }
    
    
    return resp_get;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp_eng;
    while(GetNext(fetchme)!=0){
        if(comp_eng.Compare(&fetchme, &literal, &cnf))return 1;
    }
    return 0;
}
