#include <gtest/gtest.h>
#include "DBFile.h"

#define dbFileLoc "db/nation.bin" 
#define wrongDbFileLoc "db/nationDB.bin" 
#define message "BAD!  Open did not work for db/nationDB.bin" 


TEST(CreateMethodTest, FileTypeIsHeap){ 
    DBFile file;
    ASSERT_EQ(1,file.Create(dbFileLoc,heap,NULL));
}

TEST(CreateMethodTest, FileTypeIsNonHeap) { 
    DBFile file;
    ASSERT_EQ(0,file.Create(dbFileLoc,tree,NULL));
}

TEST(OpenMethodTest, ValidPath) { 
    DBFile file;
    ASSERT_EQ(1,file.Open(dbFileLoc));
}

TEST(OpenMethodTest, InvalidPath) { 
    DBFile file;
    ASSERT_DEATH(file.Open(wrongDbFileLoc),message);
}

TEST(CloseMethodTest, OpenAndClose) { 
    DBFile file;
    file.Open(dbFileLoc);
    ASSERT_EQ(1,file.Close());
}


int main(int argc, char **argv) 
    {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
    }