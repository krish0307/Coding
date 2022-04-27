
CC = g++ -O2 -Wno-deprecated
tag = -i
test_out_tag = -std=c++11 -ll -pthread #-lgtest -lgtest_main

ifdef linux
tag = -n
# test_out_tag = -std=c++11 -pthread -lgtest -lgtest_main
endif

a42.out:   y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o NodeQueryingTree.o Statistics.o QueryOperatingTree.o main.o
	$(CC) -o a42.out y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o NodeQueryingTree.o Statistics.o QueryOperatingTree.o main.o -ll -lpthread

gtest.out: y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o NodeQueryingTree.o Statistics.o QueryOperatingTree.o testing.o
	$(CC) -o gtest.out y.tab.o lex.yy.o Record.o Schema.o Comparison.o ComparisonEngine.o Function.o Pipe.o BigQ.o File.o DBFile.o NodeQueryingTree.o Statistics.o QueryOperatingTree.o testing.o $(test_out_tag)

gtest.o: GTest.cc
	$(CC) -g -c GTest.cc  -o gtest.o

NodeQueryingTree.o: NodeQueryingTree.cc
	$(CC) -g -c NodeQueryingTree.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

Function.o: Function.cc
	$(CC) -g -c Function.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

main.o : main.cc
	$(CC) -g -c main.cc

QueryOperatingTree.o : QueryOperatingTree.cc
	$(CC) -g -c QueryOperatingTree.cc

y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h