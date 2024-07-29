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

namespace mpr
{
    
    class ProcessInfo
    {
    private:
        int myProcJob;
        int myStageID;
        bool isLateSpawnProcess;

        int64_t totalReconfigMillisecAdd;
        int64_t totalReconfigMillisecBan;

    public:
        ProcessInfo();
        ~ProcessInfo();

        int GetMyGlobalRank(MPI_Comm globalComm);

        void SetIsLateSpawnProcess(bool _isLateSpawnProcess);
        bool GetIsLateSpawnProcess();

        void SetProcJob(int _myProcJob);
        int  GetProcJob();

        void SetStageID(int _myStageID);
        int GetStageID();

        int64_t GetReconfigTimeAdd() { return totalReconfigMillisecAdd; }
        void AddReconfigTimeAdd(std::chrono::milliseconds reconfigTime) { totalReconfigMillisecAdd += reconfigTime.count(); }

        int64_t GetReconfigTimeBan() { return totalReconfigMillisecBan; }
        void AddReconfigTimeBan(std::chrono::milliseconds reconfigTime) { totalReconfigMillisecBan += reconfigTime.count(); }

    };

    ProcessInfo::ProcessInfo(){
        totalReconfigMillisecAdd = 0;
        totalReconfigMillisecBan = 0;
    }
    
    ProcessInfo::~ProcessInfo(){
    }

    int ProcessInfo::GetMyGlobalRank(MPI_Comm globalComm){
        int myGlobalRank;
        MPI_Comm_rank(globalComm, &myGlobalRank);
        return myGlobalRank;
    }


    void ProcessInfo::SetIsLateSpawnProcess(bool _isLateSpawnProcess){
        isLateSpawnProcess = _isLateSpawnProcess;
    }

    bool ProcessInfo::GetIsLateSpawnProcess(){
        return isLateSpawnProcess;
    }



    void ProcessInfo::SetProcJob(int _myProcJob){
        myProcJob = _myProcJob;
    }

    int ProcessInfo::GetProcJob(){
        return myProcJob;
    }

    void ProcessInfo::SetStageID(int _myStageID){
        myStageID = _myStageID;
    }

    int ProcessInfo::GetStageID(){
        return myStageID;
    }

}