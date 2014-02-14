all: example-c example-cpp

example-c: example.c
	$(CC) -Wall example.c -l aws_dynamo -o example-c

example-cpp: example.cpp
	$(CXX) -Wall example.cpp -l aws_dynamo -o example-cpp

clean:
	rm -f example
