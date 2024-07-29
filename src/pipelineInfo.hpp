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

// C++ includes
#include <iostream>
#include <fstream>

// External libraries
#include "../libs/nlohmann/json.hpp"

using namespace std;
using namespace nlohmann;

namespace mpr
{

    class PipelineInfo
    {
    private:
        char ** argv;
        MPI_Comm globalComm; 
        MPI_Comm pipelineComm;

        MPI_Group globalGroup;

        int numStages;
        int originalProcsSize;
        int * stageSize;
        bool orderingEnabled = false;
        
        vector<int> * stageProcessGroupRanks;
        MPI_Group * stageProcessGroups;
        MPI_Group * stageManagerGroups;

    public:
        PipelineInfo() {};
        PipelineInfo(char **argv, int pipelineSize);
        ~PipelineInfo();

        char ** GetArgv();

        int GetStageSize(int stageID);
        void SetStageSize(int stageID, int size);
        int GetAllStagesSize();

        void UpdateStageSize();
        void UpdateStageRanksUsingGlobalComm(int stageID);
        void UpdateStageRanksUsingMPILogic(int * ranksToBan, int amountBans);
        bool IsRankInStageProcessGroupRanks(int stageID, int rank);

        void EnableOrdering() { orderingEnabled = true; }
        bool IsOrderingEnabled() { return orderingEnabled; }

        int GetNumStages();
        int GetStageFirstRank(int stageID);
        int GetPipelineSize();

        void SetPipelineOriginalSize(int globalSize);
        int GetPipelineOriginalSize();

        MPI_Comm * GetGlobalComm();
        MPI_Comm * GetPipelineComm();

        void SetProcessStageGroupRanks(int stageID, int * newProcessStageRanks, int numRanks);
        void ClearAndSetProcessStageGroupRanks(int stageID, int * newProcessStageRanks, int numRanks);

        void SetStageManagerGroups();
        void SetStageProcessGroups();

        void AppendRankToProcessStage(int stageID, int rank);

        int * SelectAndRemoveHighestRanks(int stageID, int amountBans);

        int * GetPointerOfRankList(int stageID);
        int GetProcessStageGroupNumRanks(int stageID);

        MPI_Group * GetStageManagerGroup(int managerID);
        MPI_Group * GetStageProcessGroup(int stageID);
        MPI_Group * GetGlobalGroup();
        
        int GetPipelineManagerTag(int stageID);
        int GetStageManagerTag(int stageID);
        int GetInputTag(int stageID);
        int GetOutputTag(int stageID);   
    };

    PipelineInfo::PipelineInfo(char **_argv, int pipelineSize) {
        this->argv = _argv;
        numStages = pipelineSize;
        
        stageProcessGroupRanks = new vector<int>[pipelineSize+1];
        stageProcessGroups = new MPI_Group[pipelineSize+1];
        stageManagerGroups = new MPI_Group[pipelineSize+1];
        stageSize = new int[pipelineSize+1];

        ifstream parameters("../config/parameters.json");
        if (parameters.is_open()){
            json j;
            parameters >> j;
            for(int stageID = 1; stageID <= numStages; stageID++){
                stageSize[stageID] = j["stage"+to_string(stageID)].get<int>();
            }
            parameters.close();
        } else {
            cout << "Failed to open parameters.json file. Exiting now..." << endl;
            exit(-1);
        }
    }

    PipelineInfo::~PipelineInfo() {}

    char ** PipelineInfo::GetArgv(){
        return argv;
    }
    
    int PipelineInfo::GetStageFirstRank(int stageID){
        return stageProcessGroupRanks[stageID].front();
    }

    int PipelineInfo::GetStageSize(int stageID){
        return stageSize[stageID];
    }

    void PipelineInfo::SetStageSize(int stageID, int size){
        stageSize[stageID] = size;
    }

    int PipelineInfo::GetAllStagesSize(){
        int totalSize = 0;
        for(int stageID = 1; stageID <= GetNumStages(); stageID++){ 
            totalSize += stageSize[stageID]; 
        }
        return totalSize;
    }

    void PipelineInfo::UpdateStageSize(){
        for(int stageID = 1; stageID <= this->numStages; stageID++){ stageSize[stageID] = stageProcessGroupRanks[stageID].size(); }
    }

    void PipelineInfo::UpdateStageRanksUsingGlobalComm(int stageID){
        MPI_Group globalGroup;
        MPI_Comm_group(this->globalComm, &globalGroup);
        MPI_Group_incl(globalGroup, stageProcessGroupRanks[stageID].size(), &stageProcessGroupRanks[stageID].front(), &stageProcessGroups[stageID]);
    }

    // OpenMPI automatically assigns ranks contiguosly and in order, we replicate this strategy for our pipelineStageGroupsRanks
    // e.g., ranks 0,1,[2,3],4 (removing 2 and 3) -> become ranks 0,1,2
    // In OpenMPI, the process executing with rank 4 becomes rank 2 after removing ranks 2 and 3
    // this strategy can vary depending on the MPI implementation
    void PipelineInfo::UpdateStageRanksUsingMPILogic(int * ranksToBan, int amountBans) {
        for(int i=0; i<amountBans; i++){
            for(int stageID = 1; stageID <= this->numStages; stageID++){
                
                int myNewRankAcc[stageProcessGroupRanks[stageID].size()] = {0};
                int pos = 0;
                for(auto& myRank : stageProcessGroupRanks[stageID]){
                    if(myRank > ranksToBan[i]) myNewRankAcc[pos]++;
                    pos++;
                }

                pos = 0;
                for(auto& myRank : stageProcessGroupRanks[stageID]){
                    myRank -= myNewRankAcc[pos];
                    pos++;
                }
            }
        }
    }

    int PipelineInfo::GetNumStages(){
        return numStages;
    }

    int PipelineInfo::GetPipelineSize(){
        int globalSize = 0;
        MPI_Comm_size(globalComm, &globalSize);
        return globalSize;
    }

    void PipelineInfo::SetPipelineOriginalSize(int globalSize){
        originalProcsSize = globalSize;
    }

    int PipelineInfo::GetPipelineOriginalSize(){
        return originalProcsSize;
    }

    MPI_Comm * PipelineInfo::GetGlobalComm() {
        return &globalComm;
    }

    MPI_Comm * PipelineInfo::GetPipelineComm() {
        return &pipelineComm;
    }

    void PipelineInfo::SetProcessStageGroupRanks(int stageID, int * newProcessStageRanks, int numRanks) {
        for(int i = 0; i < numRanks; i++){ 
            if (std::find(stageProcessGroupRanks[stageID].begin(), stageProcessGroupRanks[stageID].end(), newProcessStageRanks[i]) == stageProcessGroupRanks[stageID].end()) {
                stageProcessGroupRanks[stageID].push_back(newProcessStageRanks[i]);
            }
        } 
    }

    void PipelineInfo::ClearAndSetProcessStageGroupRanks(int stageID, int * newProcessStageRanks, int numRanks) {
        stageProcessGroupRanks[stageID].clear();
        for(int i = 0; i < numRanks; i++){ 
            stageProcessGroupRanks[stageID].push_back(newProcessStageRanks[i]);
        }
    }

    void PipelineInfo::SetStageManagerGroups() {
        // get the globalComm group
        MPI_Group globalGroup;
        MPI_Comm_group(this->globalComm, &globalGroup);

        // Iterate all stages and for each set one process rank as stageManager
        // by default, each stageManager will carry the same rank as the stageID
        // e.g., stageManager for stage 1 will have rank 1, 
        //       stageManager for stage 2 will have rank 2, ...
        for(int managerID = 0; managerID <= this->GetNumStages(); managerID++) {
            int groupRange[1][3];
            groupRange[0][0] = managerID;
            groupRange[0][1] = managerID;
            groupRange[0][2] = 1;

            MPI_Group_range_incl(globalGroup, 1, groupRange, &(this->stageManagerGroups[managerID]));
        }
    }

    void PipelineInfo::SetStageProcessGroups() {
        MPI_Group globalGroup;
        MPI_Comm_group(this->globalComm, &globalGroup);

        int stageInit[this->GetNumStages()+1];
        int stageEnd[this->GetNumStages()+1];

        // Initialize the ranks for each stage
        // The initial ranks have already been assigned to the manager groups
        // For example, in a pipeline with 3 stages, ranks 1, 2, and 3 are assigned to the managers
        // Therefore, we begin the first stage with rank 4 (NumStages + 1 = 3 + 1)
        // Assuming stage 1 has 2 processes, it will occupy ranks from 4 up to 5 (4 + stageSize[1] - 1 = 4 + 2 - 1)
        stageInit[1] = this->GetNumStages()+1;
        stageEnd[1] = stageInit[1]+this->GetStageSize(1)-1;
        stageInit[this->GetNumStages()] = stageEnd[1]+1;
        stageEnd[this->GetNumStages()] = stageInit[this->GetNumStages()]+this->GetStageSize(this->GetNumStages())-1;
        for(int stageID = 2; stageID < this->GetNumStages(); stageID++) {
            if(stageID-1==1) {
                stageInit[stageID] = stageEnd[this->GetNumStages()]+1;
                stageEnd[stageID] = stageInit[stageID]+this->GetStageSize(stageID)-1;
            } else {
                stageInit[stageID] = stageEnd[stageID-1]+1;
                stageEnd[stageID] = stageInit[stageID]+this->GetStageSize(stageID)-1;
            }
        }

        // Pipeline Stage Groups
        for(int stageID = 1; stageID <= this->GetNumStages(); stageID++) {
            int groupRange[1][3];
            groupRange[0][0] = stageInit[stageID];
            groupRange[0][1] = stageEnd[stageID];
            groupRange[0][2] = 1;
            MPI_Group_range_incl(globalGroup, 1, groupRange, &(this->stageProcessGroups[stageID]));
            for(int localRank = groupRange[0][0]; localRank <= groupRange[0][1]; localRank += groupRange[0][2]){ this->stageProcessGroupRanks[stageID].push_back(localRank); }
        }
    }

    void PipelineInfo::AppendRankToProcessStage(int stageID, int rank){
        stageProcessGroupRanks[stageID].push_back(rank);
    }

    int * PipelineInfo::SelectAndRemoveHighestRanks(int stageID, int amountBans) {
        int * ranksToBan = new int[amountBans];
        // by default, always remove the highest ranks first
        // find which are the highest ranks and removes them from the list
        for(int i = 0; i < amountBans; i++){ 
            auto highestRank = this->stageProcessGroupRanks[stageID].begin();
            for(auto rankIter = this->stageProcessGroupRanks[stageID].begin(); rankIter != this->stageProcessGroupRanks[stageID].end(); ++rankIter) {
                if(*rankIter > *highestRank) highestRank = rankIter;
            }
            ranksToBan[i] = *highestRank;
            this->stageProcessGroupRanks[stageID].erase(highestRank);
        }
        return ranksToBan;
    }

    int * PipelineInfo::GetPointerOfRankList(int stageID) {
        return &stageProcessGroupRanks[stageID].front();
    }

    int PipelineInfo::GetProcessStageGroupNumRanks(int stageID) {
        return stageProcessGroupRanks[stageID].size();
    }

    MPI_Group * PipelineInfo::GetStageManagerGroup(int managerID) {
        return &(this->stageManagerGroups[managerID]);
    }

    MPI_Group * PipelineInfo::GetStageProcessGroup(int stageID) {
        return &(this->stageProcessGroups[stageID]);
    }

    bool PipelineInfo::IsRankInStageProcessGroupRanks(int stageID, int rank) {
        for(auto rankIter = this->stageProcessGroupRanks[stageID].begin(); rankIter != this->stageProcessGroupRanks[stageID].end(); ++rankIter) {
            if(*rankIter == rank) {
                return true;
            }
        }
        return false;
    }

    MPI_Group * PipelineInfo::GetGlobalGroup() {
        MPI_Comm_group(this->globalComm, &this->globalGroup);
        return &(this->globalGroup);
    }

    int PipelineInfo::GetPipelineManagerTag(int stageID){
        return 1000 + stageID;
    }

    int PipelineInfo::GetStageManagerTag(int stageID){
        return 1100 + stageID;
    }

    int PipelineInfo::GetInputTag(int stageID){
        return 10000 + numStages + stageID - 1;
    }

    int PipelineInfo::GetOutputTag(int stageID){
        return 10000 + numStages + stageID;
    }
}