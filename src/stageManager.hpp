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

#include "defines.hpp"
#include "structs.hpp"
#include "pipelineInfo.hpp"
#include "processInfo.hpp"
#include "serialization.hpp"

namespace mpr 
{
    class StageManager
    {
    private:

        PipelineInfo * pipeInfo;
        ProcessInfo * procInfo;
        Serialization serialization;

        int * itemsConsumed;
        int * itemsProduced;

        int interCommStageTagID, interCommManagerTagID;

        double generateStatsInterval = 1;
        MPI_Request requestPipelineManagerConfig, requestManagerStageConfig;
        int notifyProcessRemaining;
        int notifyProcessReceived = 0;
        vector<int> notifyProcessList;
        int eosProcesses = 0;
        double waitingTime;
        int memoryStageSize;

        MPI_Comm * globalComm;
        MPI_Comm * pipelineComm;

        MPI_Comm stageManagerComm, managersComm;

        int * bufConfigManager;
        StageStatus * stageStatus;

        ProcessStatus * processStatusList;
        
        std::chrono::time_point<std::chrono::steady_clock> startClock;

    public:
        StageManager(PipelineInfo * _pipeInfo, ProcessInfo * _procInfo, int myStageID);
        ~StageManager();

        void Execute(int stageID);
        void GenerateJsonStats(int myStageID, double waitingTime, int memoryStageSize);
    };

    StageManager::StageManager(PipelineInfo * _pipeInfo, ProcessInfo * _procInfo, int myStageID): pipeInfo(_pipeInfo), procInfo(_procInfo) {
        interCommStageTagID = pipeInfo->GetStageManagerTag(myStageID);
        interCommManagerTagID = pipeInfo->GetPipelineManagerTag(myStageID);

        memoryStageSize = pipeInfo->GetStageSize(myStageID);

        globalComm = pipeInfo->GetGlobalComm();
        pipelineComm = pipeInfo->GetPipelineComm();

        MPI_Intercomm_create( *pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(myStageID), interCommStageTagID, &stageManagerComm);
        MPI_Intercomm_create( *pipelineComm, 0, *globalComm, 0, interCommManagerTagID, &managersComm);

        bufConfigManager = new int[pipeInfo->GetNumStages()+1];
        stageStatus = new StageStatus();

        itemsConsumed = new int[pipeInfo->GetStageSize(myStageID)];
        itemsProduced = new int[pipeInfo->GetStageSize(myStageID)];

        for(int procRank = 0; procRank<pipeInfo->GetStageSize(myStageID); procRank++){
            itemsConsumed[procRank] = 0;
            itemsProduced[procRank] = 0;
        }
        
        bufConfigManager[0] = 0; // code 0 - nothing

        // remove finished processes from the notify step
        notifyProcessRemaining = pipeInfo->GetStageSize(myStageID) - eosProcesses;

        processStatusList = new ProcessStatus[pipeInfo->GetStageSize(myStageID)];
    }

    StageManager::~StageManager() {
    }

    void StageManager::Execute(int myStageID) {
        startClock = std::chrono::steady_clock::now();

        while(true){
            MPI_Irecv(stageStatus, 1, serialization.MPI_STAGE_STATUS, MPI_ANY_SOURCE, CONFIG_MSG, stageManagerComm, &requestManagerStageConfig);
            MPI_Wait(&requestManagerStageConfig, MPI_STATUS_IGNORE);
            
            // reply with the new Action code to the process that notified the stage manager
            if(stageStatus->code != 2) {
                MPI_Isend(bufConfigManager, pipeInfo->GetNumStages()+1, MPI_INT, stageStatus->myRank, CONFIG_MSG, stageManagerComm, &requestManagerStageConfig);
                MPI_Wait(&requestManagerStageConfig, MPI_STATUS_IGNORE);
            }
            
            // performs a quick verification to identify if the process has already communicated with the stage manager
            if (std::find(notifyProcessList.begin(), notifyProcessList.end(), stageStatus->myRank) == notifyProcessList.end()) {
                notifyProcessList.push_back(stageStatus->myRank);
                notifyProcessReceived++; // a process is alive
            }

            itemsConsumed[stageStatus->myRank] += stageStatus->itemsConsumed;
            itemsProduced[stageStatus->myRank] += stageStatus->itemsProduced;

            if(stageStatus->code == 1){
                processStatusList[stageStatus->myRank].setAlive(); // a process notified it is alive
            } else if(stageStatus->code == 2){
                eosProcesses++; // wait for one less process to notify
                notifyProcessReceived--; // to correctly monitor remaining processes to notify
                processStatusList[stageStatus->myRank].setEOS(); // a process notified EOS
            } else if(stageStatus->code == 8){
                processStatusList[stageStatus->myRank].setSpecialEOS(); // the first process notified special EOS (intent to finish)
            }

            notifyProcessRemaining = pipeInfo->GetStageSize(myStageID) - notifyProcessReceived - eosProcesses;
            
            waitingTime = std::chrono::duration<double>(std::chrono::steady_clock::now()-startClock).count();

            if (notifyProcessRemaining == 0) {
                if(bufConfigManager[0] == 3) {
                    int amountNewProcesses = 0;
                    // calculate the amount of new processes
                    for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ amountNewProcesses += (bufConfigManager[stageID] - pipeInfo->GetStageSize(stageID)); }
                    
                    for(int processRank=1; processRank<=amountNewProcesses; processRank++){
                        char ** argv = pipeInfo->GetArgv();

                        MPI_Comm interComm;
                        MPI_Comm_spawn(argv[0], argv+1, 1, MPI_INFO_NULL, 0, *globalComm, &interComm, MPI_ERRCODES_IGNORE);
                        MPI_Intercomm_merge(interComm, 0, globalComm);
                    }

                    int isMsgReady;
                    MPI_Request requestReconfig;
                    MPI_Message probeMsg;
                    MPI_Status status;
                    int numRanks;

                    // Recv and update the process stage group ranks with the new processes added in the stage
                    for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++) {
                        // Test if there is a message from the pipeline manager
                        do {
                            MPI_Improbe(0, RECONFIG_MSG, *globalComm, &isMsgReady, &probeMsg, &status);
                        } while (!isMsgReady);
                        MPI_Get_count(&status, MPI_INT, &numRanks);

                        // The pipeline manager will send the new ranks of the processes added in the stage
                        int newProcessStageRanks[numRanks];
                        MPI_Imrecv(newProcessStageRanks, numRanks, MPI_INT, &probeMsg, &requestReconfig);
                        MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);

                        // Save the updated ranks in the corresponding stageID
                        pipeInfo->SetProcessStageGroupRanks(stageID, newProcessStageRanks, numRanks);

                        // only the stages with new processes recreate the manager intercommunicator, others can use the current communicator since nothing changed
                        if((pipeInfo->GetStageSize(stageID) < pipeInfo->GetProcessStageGroupNumRanks(stageID)) && (stageID == myStageID)) {
                            MPI_Intercomm_create( *pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(stageID), interCommStageTagID, &stageManagerComm);
                        }

                        // Update the stage sizes after the new processes are added
                        pipeInfo->UpdateStageSize();

                        if(stageID == myStageID && memoryStageSize < pipeInfo->GetStageSize(stageID)){
                            // update processStatusList with new stage size;
                            ProcessStatus * processStatusListTemp = new ProcessStatus[pipeInfo->GetStageSize(stageID)];
                            memcpy(processStatusListTemp, processStatusList, sizeof(ProcessStatus)*memoryStageSize);
                            delete[] processStatusList;
                            processStatusList = processStatusListTemp;

                            // update stats with new stage size;
                            int * itemsConsumedTemp = new int[pipeInfo->GetStageSize(stageID)];
                            int * itemsProducedTemp = new int[pipeInfo->GetStageSize(stageID)];

                            memcpy(itemsConsumedTemp, itemsConsumed, sizeof(int)*memoryStageSize);
                            memcpy(itemsProducedTemp, itemsProduced, sizeof(int)*memoryStageSize);
                             
                            delete[] itemsConsumed;
                            delete[] itemsProduced;

                            itemsConsumed = itemsConsumedTemp;
                            itemsProduced = itemsProducedTemp;
                            
                            // we always keep the previous stored stats and initialize only the new ones in zero
                            for(int procRank = memoryStageSize; procRank<pipeInfo->GetStageSize(stageID); procRank++){
                                itemsConsumed[procRank] = 0;
                                itemsProduced[procRank] = 0;
                            }

                            // keep a memory about the maximum size of stats we are storing
                            memoryStageSize = pipeInfo->GetStageSize(stageID);
                        }
                    }

                    MPI_Barrier(*globalComm);

                } else if (bufConfigManager[0] == 4) {
                    int isMsgReady;
                    MPI_Request requestReconfig;
                    MPI_Message probeMsg;
                    MPI_Status status;
                    int numRanks;
                    int banListSize;
                    
                    // Test if there is a message from the pipeline manager
                    do {
                        MPI_Improbe(0, RECONFIG_MSG, *globalComm, &isMsgReady, &probeMsg, &status);
                    } while (!isMsgReady);
                    MPI_Get_count(&status, MPI_INT, &banListSize);
                    
                    // The pipeline manager will send the ranks of the processes chosen to be removed from the stage
                    int ranksToBan[banListSize];
                    MPI_Imrecv(ranksToBan, banListSize, MPI_INT, &probeMsg, &requestReconfig);
                    MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);

                    // Recv and update the process stage group ranks with the new ranks send by the pipeline manager
                    for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++) {
                        // Test if there is a message from the pipeline manager
                        do {
                            MPI_Improbe(0, RECONFIG_MSG, *globalComm, &isMsgReady, &probeMsg, &status);
                        } while (!isMsgReady);
                        MPI_Get_count(&status, MPI_INT, &numRanks);

                        // The pipeline manager will send an updated list of process ranks in the stage
                        int newProcessStageRanks[numRanks];
                        MPI_Imrecv(newProcessStageRanks, numRanks, MPI_INT, &probeMsg, &requestReconfig);
                        MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);

                        // Clear and save the updated ranks in the corresponding stageID
                        pipeInfo->ClearAndSetProcessStageGroupRanks(stageID, newProcessStageRanks, numRanks);
                    }
                           
                    // only stage manager from banned stages should wait for one last message sent by each process that will be banned
                    if(bufConfigManager[myStageID] < pipeInfo->GetStageSize(myStageID)){
                        for(int procRank=0; procRank<banListSize; procRank++){
                            MPI_Irecv(stageStatus, 1, serialization.MPI_STAGE_STATUS, MPI_ANY_SOURCE, BAN_MSG, stageManagerComm, &requestManagerStageConfig);
                            MPI_Wait(&requestManagerStageConfig, MPI_STATUS_IGNORE);
                            
                            itemsConsumed[stageStatus->myRank] += stageStatus->itemsConsumed;
                            itemsProduced[stageStatus->myRank] += stageStatus->itemsProduced;
                        }
                    }

                    // Obtain the group of processes in the world communicator
                    MPI_Group newGlobalGroup;
                    MPI_Group * globalGroup = pipeInfo->GetGlobalGroup();

                    // Remove all banned ranks
                    MPI_Group_excl(*globalGroup, banListSize, ranksToBan, &newGlobalGroup);
                    // Replace the global communicator
                    MPI_Comm_create(*globalComm, newGlobalGroup, globalComm);

                    for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ 
                        if(stageID == myStageID) {
                            MPI_Intercomm_create(*pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(stageID), interCommStageTagID, &stageManagerComm);
                        }

                        // Update the stage sizes after the processes are removed
                        pipeInfo->UpdateStageSize();

                        if(stageID == myStageID){
                            // shrink and persist processStatusList control variable;
                            ProcessStatus * processStatusListTemp = new ProcessStatus[pipeInfo->GetStageSize(stageID)];
                            memcpy(processStatusListTemp, processStatusList, sizeof(ProcessStatus)*pipeInfo->GetStageSize(stageID));
                            processStatusList = processStatusListTemp;
                        }
                    } 

                    MPI_Barrier(*globalComm);
                }

                waitingTime = std::chrono::duration<double>(std::chrono::steady_clock::now()-startClock).count();

                if(waitingTime >= generateStatsInterval){
                    GenerateJsonStats(myStageID, waitingTime, pipeInfo->GetStageSize(myStageID));
                    startClock = std::chrono::steady_clock::now();
                }

                // report status to pipeline manager
                MPI_Isend(processStatusList, pipeInfo->GetStageSize(myStageID), serialization.MPI_PROCESS_STATUS, 0, MANAGER_MSG, managersComm, &requestPipelineManagerConfig);
                MPI_Wait(&requestPipelineManagerConfig, MPI_STATUS_IGNORE);

                // recv config code from pipeline manager
                MPI_Irecv(bufConfigManager, pipeInfo->GetNumStages()+1, MPI_INT, 0, MANAGER_MSG, managersComm, &requestPipelineManagerConfig);
                MPI_Wait(&requestPipelineManagerConfig, MPI_STATUS_IGNORE);

                // already sent the current report, clear and reuse for the next one
                notifyProcessReceived = 0;
                notifyProcessList.clear();

                // can only exit after ensuring the pipeline manager received all EOS
                bool stageFinished = true;
                for(int i = 0; i < pipeInfo->GetStageSize(myStageID); i++) {
                    stageFinished &= processStatusList[i].getEOS();
                }

                if(stageFinished && bufConfigManager[0] == 7) { break; }
            }
        }
        
        waitingTime = std::chrono::duration<double>(std::chrono::steady_clock::now()-startClock).count();

        // before exiting we save the stats one last time
        GenerateJsonStats(myStageID, waitingTime, pipeInfo->GetStageSize(myStageID));

        MPI_Barrier(*globalComm);
    }

    void StageManager::GenerateJsonStats(int myStageID, double waitingTime, int numProcs){
        ifstream fileIn("../config/stats_stage"+to_string(myStageID)+".json");
        json j;
        if(!fileIn.fail()){
            fileIn >> j;
        }
        fileIn.close();

        std::chrono::milliseconds ms = std::chrono::duration_cast< std::chrono::milliseconds >(
            std::chrono::system_clock::now().time_since_epoch()
        );
        auto timestamp = ms.count();

        bool debugging = false;
        for(int procRank= 0; procRank<numProcs; procRank++){
            // Source does not consume items
            if(myStageID != 1){
                j["stage"+to_string(myStageID)][to_string(timestamp)]["Proc"+to_string(procRank)]["ItemsConsumed"] = itemsConsumed[procRank];
                j["stage"+to_string(myStageID)][to_string(timestamp)]["Proc"+to_string(procRank)]["averageItemsConsumed"] = itemsConsumed[procRank]/waitingTime;
            }
            // Sink does not produce items
            if(myStageID != pipeInfo->GetNumStages()){
                j["stage"+to_string(myStageID)][to_string(timestamp)]["Proc"+to_string(procRank)]["ItemsProduced"] = itemsProduced[procRank];
                j["stage"+to_string(myStageID)][to_string(timestamp)]["Proc"+to_string(procRank)]["averageItemsProduced"] = itemsProduced[procRank]/waitingTime;
            }

            itemsConsumed[procRank] = 0;
            itemsProduced[procRank] = 0;
        }
        
        ofstream fileIn2("../config/stats_stage"+to_string(myStageID)+".json");
        fileIn2 << setw(4) << j << endl;
        fileIn2.close();
    }
}