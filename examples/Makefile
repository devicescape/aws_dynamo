all: v2-example-c example-c example-cpp

example-c: example.c
	$(CC) -Wall example.c -l aws_dynamo -o example-c

v2-example-c: v2-example.c
	$(CC) -Wall v2-example.c -l aws_dynamo -o v2-example-c

example-cpp: example.cpp
	$(CXX) -Wall example.cpp -l aws_dynamo -o example-cpp

clean:
	rm -f v2-example-c example-c example-cpp
