#include "Statistics.h"
#include <map>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <fstream>
#include <iostream>

class QueryOperatingTree {
public:
    void fetchSelectsAndJoins(vector <AndList> &joins, vector <AndList> &selects, vector <AndList> &joinDepSel,
                                   Statistics s);
    void fetchTables(vector<string>& relations);
    QueryOperatingTree();
};
