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


// Action codes:
// code 0 - Notification to maintain the same course of action without any variations.
//          Sent from Pipeline Manager to all processes and from Stage Manager to Stage Process. 
//          Requires two-sided synchronization.

// code 1 - Process alive notification. Processes also send their status containing
//          the number of items consumed and produced. Sent from Stage Process to Stage
//          Manager. Requires two-sided synchronization.

// code 2 - Process EOS (End of Stream) notification. Processes also send their status
//          containing the number of items consumed and produced. Sent from Stage Process
//          to Stage Manager. Requires two-sided synchronization.

// code 3 - Reconfiguration notification. Synchronization is required to ADD processes to the 
//          Pipeline graph. Sent from Pipeline Manager to Stage Manager and
//          from Stage Manager to Stage Process. Requires global synchronization.

// code 4 - Reconfiguration notification. Synchronization is required to BAN processes
//          from the Pipeline graph. Sent from Pipeline Manager to Stage Manager and from
//          Stage Manager to Stage Process. Requires global synchronization.

// code 5 - Reconfiguration notification complementary to Action 3. All newly spawned processes 
//          receive information about which stages need processes and which stages have enough processes. 
//          Sent from PipelineManager to NEW Stage Processes. Requires partial synchronization.

// code 6 - Spawn notification. All SPAWNED processes receive information on how
//          many processes are to be spawned. Requires partial synchronization.

// code 7 - Pipeline is ending notification. After receiving, processes can finish safely and no 
//          more adaptations are allowed to the pipeline. Sent from Pipeline Manager to all 
//          processes and from Stage Manager to Stage Process. Requires two-sided synchronization.

// code 8 - Special process EOS (End of Stream) notification. Sent from Stage Process
//          to Stage Manager. Requires two-sided synchronization.


namespace mpr 
{

    class PipelineManager
    {
    private:
        double checkInterval = 1;
        double waitingTime;
        MPI_Request requestConfig, requestSend;
        int skippedStage = 0;
        MPI_Message probeMsg;            
        bool pipelineEnding = false;

        std::chrono::time_point<std::chrono::steady_clock> startReconfig;
        std::chrono::time_point<std::chrono::steady_clock> endReconfig;

        MPI_Comm * globalComm ;
        MPI_Comm * pipelineComm;

        MPI_Comm * managersComm;

        int * bufConfig;
        int * bufConfigAux;
        int * newStageSize;
        bool * managerNotify;
        ProcessStatus ** processStatusList;

        std::chrono::time_point<std::chrono::steady_clock> startClock;

        PipelineInfo * pipeInfo;
        ProcessInfo * procInfo;
        Serialization serialization;

    public:
        PipelineManager(PipelineInfo * _pipeInfo, ProcessInfo * _procInfo);
        ~PipelineManager();

        void Execute();

    };

    PipelineManager::PipelineManager(PipelineInfo * _pipeInfo, ProcessInfo * _procInfo): pipeInfo(_pipeInfo), procInfo(_procInfo)  {
        globalComm = pipeInfo->GetGlobalComm();
        pipelineComm = pipeInfo->GetPipelineComm();
        managersComm = new MPI_Comm[pipeInfo->GetNumStages()+1];

        for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){
            int interCommTagID = pipeInfo->GetPipelineManagerTag(stageID);
            MPI_Intercomm_create(*pipelineComm, 0, *globalComm, stageID, interCommTagID, &managersComm[stageID]);
        }

        bufConfig = new int[pipeInfo->GetNumStages()+1];
        bufConfigAux = new int[pipeInfo->GetNumStages()+1];
        newStageSize = new int[pipeInfo->GetNumStages()+1];
        managerNotify = new bool[pipeInfo->GetNumStages()+1];
        processStatusList = new ProcessStatus*[pipeInfo->GetNumStages()+1];

        for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){
            processStatusList[stageID] = new ProcessStatus[pipeInfo->GetStageSize(stageID)];
            newStageSize[stageID] = pipeInfo->GetStageSize(stageID);
            managerNotify[stageID] = false;
        }
    }

    PipelineManager::~PipelineManager(){ }

    void PipelineManager::Execute() {
        startClock = std::chrono::steady_clock::now();
        while(true){
            int notifyStageRemaining = 0;
            int isMsgReady = 0;
            int msgSourceID = 0;
            do {
                for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){
                    MPI_Improbe(MPI_ANY_SOURCE, MANAGER_MSG, managersComm[stageID], &isMsgReady, &probeMsg, MPI_STATUS_IGNORE);
                    msgSourceID = stageID;
                    if(isMsgReady) {break;}
                }
            } while (!isMsgReady);

            // Recv status from the stage managers
            MPI_Imrecv(processStatusList[msgSourceID], pipeInfo->GetStageSize(msgSourceID), serialization.MPI_PROCESS_STATUS, &probeMsg, &requestConfig);
            MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);


            bool stageFinished = true;
            for(int i = 0; i < pipeInfo->GetStageSize(msgSourceID); i++) {
                stageFinished &= processStatusList[msgSourceID][i].getEOS();
                if(processStatusList[msgSourceID][i].getSpecialEOS()){
                    pipelineEnding = true;
                }
            }
            if(stageFinished) { skippedStage++; }

            // code 0 - nothing new to report for the stage managers
            bufConfig[0] = 0;

            if(pipelineEnding) {
                // pipeline is ending since one of the processes finished its job
                // from now on, always send the code 7 to all processes
                // also, adaptability is not allowed anymore.
                bufConfig[0] = 7;
            }

            waitingTime = std::chrono::duration<double>(std::chrono::steady_clock::now()-startClock).count();
            // Time to check if the number of processes was modified in json file
            if(waitingTime >= checkInterval && !pipelineEnding){
                try{
                    ifstream parameters("../config/parameters.json");
                    if (parameters.is_open()){
                        json j;
                        parameters >> j;
                        for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){
                            newStageSize[stageID] = j["stage"+to_string(stageID)].get<int>();
                        }
                        parameters.close();
                    } else {
                        cout << "Failed to open parameters.json file. Exiting now..." << endl;
                        exit(-1);
                    }
                } catch (json::parse_error& ex){
                    continue;
                }

                startClock = std::chrono::steady_clock::now();
            }

            // check if the values read from the json file are bigger than the current values
            // if so, we need to add new processes to the pipeline
            if((newStageSize[1] > pipeInfo->GetStageSize(1) || newStageSize[2] > pipeInfo->GetStageSize(2) || newStageSize[3] > pipeInfo->GetStageSize(3)) && managerNotify[msgSourceID] == false && skippedStage == 0 && !pipelineEnding){ 
                // code 3 - synchronize to add processes to pipeline
                bufConfig[0] = 3;
                for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ bufConfig[stageID] = newStageSize[stageID]; }
                managerNotify[msgSourceID] = true;
            } 
            // check if the values read from the json file are smaller than the current values
            // if so, we need to remove processes from the pipeline
            else if((newStageSize[1] < pipeInfo->GetStageSize(1) || newStageSize[2] < pipeInfo->GetStageSize(2) || newStageSize[3] < pipeInfo->GetStageSize(3)) && managerNotify[msgSourceID] == false && skippedStage == 0 && !pipelineEnding){ 
                // code 4 - synchronize to remove processes from pipeline
                bufConfig[0] = 4;
                for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ bufConfig[stageID] = newStageSize[stageID]; }
                managerNotify[msgSourceID] = true;
            }
            
            // Send a message with the choosen Action to the Managers
            MPI_Isend(bufConfig, pipeInfo->GetNumStages()+1, MPI_INT, 0, MANAGER_MSG, managersComm[msgSourceID], &requestSend);
            MPI_Wait(&requestSend, MPI_STATUS_IGNORE);
        
            bool managersNotified = true;
            for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ managersNotified &= managerNotify[stageID]; }

            if(managersNotified) {
                if(bufConfig[0] == 3) { 
                    startReconfig = std::chrono::steady_clock::now();

                    int amountNewProcesses = 0;
                    for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ 
                        managerNotify[stageID] = false; 
                        // calculate the amount of new processes to be added
                        amountNewProcesses += (newStageSize[stageID] - pipeInfo->GetStageSize(stageID));
                        bufConfig[stageID] = 0;
                    }

                    for(int processRank=1; processRank<=amountNewProcesses; processRank++){
                        char ** argv = pipeInfo->GetArgv();
                        
                        MPI_Comm interComm;
                        MPI_Comm_spawn(argv[0], argv+1, 1, MPI_INFO_NULL, 0, *globalComm, &interComm, MPI_ERRCODES_IGNORE);
                        MPI_Intercomm_merge(interComm, 0, globalComm);

                        bufConfigAux[0] = 6;
                        bufConfigAux[1] = amountNewProcesses-processRank;

                        MPI_Isend(bufConfigAux, pipeInfo->GetNumStages()+1, MPI_INT, pipeInfo->GetPipelineSize()-1, RECONFIG_MSG, *globalComm, &requestSend);
                        MPI_Wait(&requestSend, MPI_STATUS_IGNORE);

                    }

                    // code 5
                    bufConfig[0] = 5;

                    for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ 
                        // find out which is the stage that is adding new processes
                        if(pipeInfo->GetStageSize(stageID) < newStageSize[stageID]) {
                            // only the stage that is adding new processes is set to 1, others are set to 0
                            bufConfig[stageID] = 1;
                            for(int localRank = pipeInfo->GetPipelineSize()-1; localRank > pipeInfo->GetPipelineSize()-1-amountNewProcesses; localRank--){ 
                                pipeInfo->AppendRankToProcessStage(stageID, localRank);    
                            }

                            // update the array that depends on the size of each stage    
                            ProcessStatus * processStatusTemp = new ProcessStatus[newStageSize[stageID]];
                            memcpy(processStatusTemp, processStatusList[stageID], sizeof(ProcessStatus)*pipeInfo->GetStageSize(stageID));
                            delete[] processStatusList[stageID];
                            processStatusList[stageID] = processStatusTemp;
                        }
                    }
                    // Update the stage sizes after the new processes are added
                    pipeInfo->UpdateStageSize();

                    MPI_Request requestReconfigNewProcesses[amountNewProcesses];
                    MPI_Request requestReconfigAllProcesses[pipeInfo->GetPipelineSize()-1];

                    // send the recovery config code to the newly spawned processes in the order N, N-1, N-2,...
                    for(int processRank = 1; processRank <= amountNewProcesses; processRank++){
                        MPI_Isend(bufConfig, pipeInfo->GetNumStages()+1, MPI_INT, pipeInfo->GetPipelineSize()-processRank, RECONFIG_MSG, *globalComm, &requestReconfigNewProcesses[processRank-1]);
                    }
                    MPI_Waitall(amountNewProcesses, requestReconfigNewProcesses, MPI_STATUS_IGNORE);

                    // send the list with updated ranks to all processes in the pipeline
                    for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ 
                        for(int localRank = 1; localRank < pipeInfo->GetPipelineSize(); localRank++){
                            MPI_Isend(pipeInfo->GetPointerOfRankList(stageID), pipeInfo->GetStageSize(stageID), MPI_INT, localRank, RECONFIG_MSG, *globalComm, &requestReconfigAllProcesses[localRank-1]);
                        }
                        MPI_Waitall(pipeInfo->GetPipelineSize()-1, requestReconfigAllProcesses, MPI_STATUS_IGNORE);
                    }
                    MPI_Barrier(*globalComm);
                    endReconfig = std::chrono::steady_clock::now();
                    
                    std::chrono::duration< double > reconfigTime = endReconfig - startReconfig;
                    std::chrono::milliseconds reconfigMillisec = std::chrono::duration_cast< std::chrono::milliseconds >( reconfigTime );
                    
                    procInfo->AddReconfigTimeAdd(reconfigMillisec);
                    
                } else if (bufConfig[0] == 4) {
                    startReconfig = std::chrono::steady_clock::now();

                    int amountBanProcesses = 0;
                    
                    MPI_Group * globalGroup = pipeInfo->GetGlobalGroup();
                    
                    for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ 
                        managerNotify[stageID] = false; 
                        // calculate the amount of banned processes
                        amountBanProcesses += (pipeInfo->GetStageSize(stageID) - newStageSize[stageID]);
                        bufConfig[stageID] = 0;
                    }
                    
                    int * ranksToBan = new int[amountBanProcesses];
                    for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ 
                        // find which one is the stage that is banning processes
                        if(newStageSize[stageID] < pipeInfo->GetStageSize(stageID)) {
                            // find which are the highest ranks and removes them from the list
                            ranksToBan = pipeInfo->SelectAndRemoveHighestRanks(stageID, amountBanProcesses);
                        }
                    }
                    pipeInfo->UpdateStageSize();
                    
                    // assign the new ranks of processes
                    pipeInfo->UpdateStageRanksUsingMPILogic(ranksToBan, amountBanProcesses);
                    
                    MPI_Request requestReconfigAllProcesses[pipeInfo->GetPipelineSize()-1];
                    
                    // send the ranksToBan list to all processes in the pipeline
                    for(int localRank = 1; localRank < pipeInfo->GetPipelineSize(); localRank++){
                        MPI_Isend(ranksToBan, amountBanProcesses, MPI_INT, localRank, RECONFIG_MSG, *globalComm, &requestReconfigAllProcesses[localRank-1]);
                    }
                    MPI_Waitall(pipeInfo->GetPipelineSize()-1, requestReconfigAllProcesses, MPI_STATUS_IGNORE);

                    // send the list with updated ranks to all processes in the pipeline
                    for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ 
                        for(int localRank = 1; localRank < pipeInfo->GetPipelineSize(); localRank++){
                            MPI_Isend(pipeInfo->GetPointerOfRankList(stageID), pipeInfo->GetStageSize(stageID), MPI_INT, localRank, RECONFIG_MSG, *globalComm, &requestReconfigAllProcesses[localRank-1]);
                        }
                        MPI_Waitall(pipeInfo->GetPipelineSize()-1, requestReconfigAllProcesses, MPI_STATUS_IGNORE);
                    }
                    
                    MPI_Group newGlobalGroup;        
                    // Remove all banned ranks
                    MPI_Group_excl(*globalGroup, amountBanProcesses, ranksToBan, &newGlobalGroup);
                    delete[] ranksToBan;
                    
                    // Replace the global communicator
                    MPI_Comm_create(*globalComm, newGlobalGroup, globalComm);

                    MPI_Barrier(*globalComm);

                    // Update the stage sizes after the new processes are added
                    pipeInfo->UpdateStageSize();
                    
                    endReconfig = std::chrono::steady_clock::now();

                    std::chrono::duration< double > reconfigTime = endReconfig - startReconfig;
                    std::chrono::milliseconds reconfigMillisec = std::chrono::duration_cast< std::chrono::milliseconds >( reconfigTime );

                    procInfo->AddReconfigTimeBan(reconfigMillisec);
                }
            }

            // remove skipped processes from the notify step
            notifyStageRemaining += pipeInfo->GetNumStages() - skippedStage;

            if (notifyStageRemaining == 0) { break; }
        }

        MPI_Barrier(*globalComm);
    }

}