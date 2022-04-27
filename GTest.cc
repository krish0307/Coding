#include <iostream>
#include <gtest/gtest.h>
#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>

#include "NodeQueryingTree.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryOperatingTree.h"

extern "C"
{
int yyparse(void);
struct YY_BUFFER_STATE* yy_scan_string(const char*);

}
extern struct FuncOperator* finalFunction;
extern struct TableList* tables;
extern struct AndList* boolean;
extern struct NameList* groupingAtts;
extern struct NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

Statistics* s = new Statistics();


TEST(QueryTreeOperationsTests, getJoinsAndSelectTest) {
char* cnf = "SELECT c.c_name FROM customer AS c, nation AS n, region AS r WHERE(c.c_nationkey = n.n_nationkey) AND (n.n_regionkey = r.r_regionkey)";
yy_scan_string(cnf);
yyparse();
QueryOperatingTree *queryTreeOperations = new QueryOperatingTree();

vector<AndList> joinlist, havingList, whereList;
Statistics* statistics = new Statistics();
queryTreeOperations->fetchSelectsAndJoins(joinlist, havingList, whereList,  *statistics);

ASSERT_TRUE(joinlist.size() == 2 && whereList.size() == 0);
delete queryTreeOperations;
}


//Check if the table name is parsed correctly from the attribute
TEST(QueryTreeOperationsTests, getTablesTest)
{
QueryOperatingTree *queryTreeOperations = new QueryOperatingTree();

Statistics* s = new Statistics();
s->Read("Statistics.txt");
vector<string> relations;
queryTreeOperations->fetchTables(relations);

ASSERT_EQ(relations.size(), 3);
delete queryTreeOperations;
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    s->Read("Statistics.txt");
    return RUN_ALL_TESTS();
}
