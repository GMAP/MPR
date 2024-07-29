/*
 __  __ ____ ____
|  \/  |    |    |   Message Passing Runtime - v1.0.0
|      |  __|    |   https://github.com/GMAP/MPR
|__||__|_|  |_|\_\   Author: Júnior Löff


MIT License

Copyright (c) 2024 Parallel Applications Modelling Group - GMAP

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifdef WINDOWS
#include <mpi.h>
#endif

#ifdef MACOSX
#include "mpi.h"
#endif

#ifndef MACOSX
#ifndef WINDOWS
#include "mpi.h"
#endif
#endif

// C++ includes
#include <cstdlib>
#include <cstdio>
#include <chrono>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <cstring>
#include <vector>
#include <queue>
#include <climits>
#include <unistd.h>

// External libraries
#include "../libs/nlohmann/json.hpp"

// Internal files
#include "pipelineGraph.hpp"

using namespace std;
using namespace nlohmann;

namespace mpr
{

    class MPR {
    private:
        PipelineGraph pipeGraph;

    public:
        MPR(int *argc, char ***argv): pipeGraph(argc, argv, 0) {
            cout << "MPR is initializing..." << endl;
        }
        ~MPR() {}
    };
}