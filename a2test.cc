#include <iostream>
#include "DBFile.h"
#include "a2test.h"
#include <iomanip>
#include <sys/time.h>
// make sure that the file path/dir information below is correct
const char *dbfile_dir = ""; // dir where binary heap files should be stored
//const char *tpch_dir ="/Users/foram_nirmal/CLionProjects/DBI/data/tpch-dbgen/"; // dir where dbgen tpch files (extension *.tbl) can be found
const char *catalog_path = "catalog"; // full path of the catalog file
const char *tpch_dir ="/Users/juhi/CLionProjects/DBI_PR1/Data/tpch-dbgen/";

using namespace std;

relation *rel;

// load from a tpch file
void test1 () {

    DBFile dbfile;
    cout << " DBFile will be created at " << rel->path () << endl;
    dbfile.Create (rel->path(), heap, NULL);

    char tbl_path[100]; // construct path of the tpch flat text file
    sprintf (tbl_path, "%s%s.tbl", tpch_dir, rel->name());
    cout << " tpch file will be loaded from " << tbl_path << endl;

    dbfile.Load (*(rel->schema ()), tbl_path);
    dbfile.Close ();
}

// sequential scan of a DBfile
void test2 () {
    const char *filePath = "tempfile.bin";
    DBFile dbfile;
//    dbfile.Open (rel->path());
    dbfile.Open(filePath);
    dbfile.MoveFirst ();

    Record temp;

    int counter = 0;
    while (dbfile.GetNext (temp) == 1) {
        counter += 1;
        temp.Print (rel->schema());
        if (counter % 10000 == 0) {
            cout << counter << "\n";
        }
    }
    cout << " scanned " << counter << " recs \n";
    dbfile.Close ();
}

// scan of a DBfile and apply a filter predicate
void test3 () {
//    time_t start,end;
//    struct timeval s,e;
    cout << " Filter with CNF for : " << rel->name() << "\n";
    CNF cnf;
    Record literal;
    rel->get_cnf (cnf, literal);
//    gettimeofday(&s, NULL);
//    long int start = s.tv_sec * 1000 + s.tv_usec / 1000;
    DBFile dbfile;
    dbfile.Open (rel->path());
    dbfile.MoveFirst ();

    Record temp;

    int counter = 0;
    while (dbfile.GetNext (temp, cnf, literal) == 1) {
        counter += 1;
        temp.Print (rel->schema());
        if (counter % 10000 == 0) {
            cout << counter << "\n";
        }
    }
    cout << " selected " << counter << " recs \n";
    dbfile.Close ();
//    gettimeofday(&e, NULL);
//    long int end = e.tv_sec * 1000 + e.tv_usec / 1000;
//	long time_taken = end-start;
//	cout << "Time taken to execute the query: " << time_taken << "milliseconds \n";
////	cout << "time taken:" << time_taken << set_precision << "\n";
}

int main () {

    setup (catalog_path, dbfile_dir, tpch_dir);

    void (*test) ();
    relation *rel_ptr[] = {n, r, c, p, ps, o, li, s};
    void (*test_ptr[]) () = {&test1, &test2, &test3};

    int tindx = 0;
    while (tindx < 1 || tindx > 3) {
        cout << " select test: \n";
        cout << " \t 1. load file \n";
        cout << " \t 2. scan \n";
        cout << " \t 3. scan & filter \n \t ";
        cin >> tindx;
    }

    int findx = 0;
    while (findx < 1 || findx > 8) {
        cout << "\n select table: \n";
        cout << "\t 1. nation \n";
        cout << "\t 2. region \n";
        cout << "\t 3. customer \n";
        cout << "\t 4. part \n";
        cout << "\t 5. partsupp \n";
        cout << "\t 6. orders \n";
        cout << "\t 7. lineitem \n";
        cout << "\t 8. supplier \n";
        cin >> findx;
    }

    rel = rel_ptr [findx - 1];
    test = test_ptr [tindx - 1];

    test ();

    cleanup ();
}
