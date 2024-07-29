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

#pragma once

// MPI
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

#include "pipelineInfo.hpp"
#include "processInfo.hpp"

namespace mpr{
    
    class Ctx
    {
        private:
            int targetProc;
            int sourceProc;
            int * bufConfig;
            StageStatus * stageStatus;
            Header * header;
            MPI_Status msgStatus;
            MPI_Message probeMsg;


        public:
            std::chrono::time_point<std::chrono::steady_clock> startClock;
            double waitingTime;


            Ctx() {}
            ~Ctx() {}

            void Init(int numStages) {
                startClock = std::chrono::steady_clock::now();
                bufConfig = new int[numStages+1];
                stageStatus = new StageStatus;
                header = new Header;
                targetProc = 0;
            }
            void SetTargetProc(int _targetProc) { targetProc = _targetProc; }
            int GetTargetProc() { return targetProc; }
            void IncrementTargetProc() { targetProc++; }

            void SetSourceProc(int _sourceProc) { sourceProc = _sourceProc; }
            int GetSourceProc() { return sourceProc; }
            void IncrementSourceProc() { sourceProc++; }

            int * GetBufConfig() { return bufConfig; }
            StageStatus * GetStageStatus() { return stageStatus; }

            void InitStartClock() { startClock = std::chrono::steady_clock::now(); }
            std::chrono::time_point<std::chrono::steady_clock> GetStartClock() { return startClock; }

            double GetWaitingTime() { return waitingTime; }
            void SetWaitingTime(double _waitingTime) { waitingTime = _waitingTime; }

            Header * GetHeader() { return header; }
            void SetHeader(Header * _header) { header = _header; }

            void SetMsgStatus(MPI_Status _msgStatus) { msgStatus = _msgStatus; }
            MPI_Status GetMsgStatus() { return msgStatus; }

            void SetProbeMsg(MPI_Message _probeMsg) { probeMsg = _probeMsg; }
            MPI_Message GetProbeMsg() { return probeMsg; }
    };
}
