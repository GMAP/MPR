#include <cstdlib>
#include <cstdio>
#include <chrono>
#include <iostream>
#include <getopt.h>
#include <cstring>
#include <sys/time.h>
#include <math.h>
#include <chrono>

#include "../../src/mpr.hpp"


class Source : public mpr::StageProcess<void*,int> {
private:
	int totalNums;
public:

	Source(int n) : totalNums(n) {}

    void OnInit(void * ctx) {}
    void OnInput(void * ctx, void * data) {}
	void OnEnd(void * ctx) {}

	void OnProduce(void * ctx) {
		for (int i = 2; i <= totalNums; i++) {
			Produce(ctx, &i, sizeof(i));
		}
	};
};

class Compute : public mpr::StageProcess<int, bool>
{
public:

    void OnInit(void * ctx) {}
    void OnProduce(void * ctx) {}
	void OnEnd(void * ctx) {}

	void OnInput(void * ctx, void * data) {
        int * dataIn = static_cast<int*>(data);

		bool isPrime = true;
		for (int j = 2; j < *dataIn; j++) {
			if (*dataIn % j == 0) {
				isPrime = false;
				break;
			}
		}
		Publish(ctx, &isPrime, sizeof(isPrime));
	};
};

class Sink : public mpr::StageProcess<bool, void*> {
private:
	int primes = 0;
public:

    void OnInit(void * ctx) {}
    void OnProduce(void * ctx) {}

	void OnInput(void * ctx, void * data) {
        bool * dataIn = static_cast<bool*>(data);
		if (*dataIn) {
			primes++;
		}
	};

	void OnEnd(void * ctx) {
		std::cout << "Primes: " << primes << std::endl;
	};
};

int main(int argc, char **argv) {

	if(argc < 2) {
        std::cout << "Usage: " << argv[0] << " [N]" << std::endl;
        return 0;
    }
	int n = atoi(argv[1]);

    mpr::PipelineGraph pipe(&argc, &argv, 3);

    Source * source = new Source(n);
    Compute * compute = new Compute();
    Sink * sink = new Sink();

    pipe.AddPipelineStage(source);
    pipe.AddPipelineStage(compute);
    pipe.AddPipelineStage(sink);
    
    pipe.Run();

	return 0; 
} 