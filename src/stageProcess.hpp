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
#include "serialization.hpp"
#include "pipelineInfo.hpp"
#include "stageInfo.hpp"
#include "stageProcessCtx.hpp"


namespace mpr
{
    class Operator {
    public:
        Operator() {}
        ~Operator() {}

        virtual void SetPipelineInfo(PipelineInfo * _pipeInfo) = 0;
        virtual void SetProcessInfo(ProcessInfo * _procInfo) = 0;
        virtual void Init(int procJob) = 0;
        virtual int Execute(int procJob) = 0;
        virtual bool IsOrderingEnabled() = 0;
    };

    template <typename TDataIn, typename TDataOut>
    class StageInfo;

    template <typename TDataIn, typename TDataOut>
    class StageProcess : public Operator
    {
    private:
        double checkInterval = 1;
        double pendingDataCheckInterval = 0.1;
        PipelineInfo * pipeInfo;
        ProcessInfo * procInfo;
        Serialization serialization;
        StageInfo<TDataIn, TDataOut> * stageInfo;
        unsigned char emptyBuffer;

        MPI_Comm * globalComm;
        MPI_Comm * pipelineComm;
        MPI_Comm * stageManagerComm;
        MPI_Comm * inputComm;
        MPI_Comm * outputComm;
        
        MPI_Request requestConfig, requestReconfig, requestOndemand;
        MPI_Request requestStop;
        MPI_Request requestOndemandSolo;

        int isMsgReady;
        int isMsgReadyOndemandSolo;
        int isRequestMsgReadyOndemandSolo;
        int isDataMsgReady;
        int isManagerMsgReady;

        MPI_Status status;
        MPI_Status statusOndemandSolo;
        MPI_Status statusReconfig;

        MPI_Message probeMsg;
        MPI_Message probeMsgOndemandSolo;
        MPI_Message probeMsgReconfig;

        double waitingTime;
        int * bufConfig;
        int myLocalRank;
        int numRanks;
        int banListSize;
        StageStatus * stageStatus;

        std::chrono::time_point<std::chrono::steady_clock> startClock;

        bool * endMsgFound;
        int sourceStageID;
        bool EOS;
        bool endingPipeline;

        Ctx ctxBase;


        StageProcess(PipelineInfo * _pipeInfo, ProcessInfo * _procInfo): pipeInfo(_pipeInfo), procInfo(_procInfo) {
            stageInfo = new StageInfo<TDataIn, TDataOut>(pipeInfo, procInfo, this);
        }
    
    public: 
        StageProcess() {
            pipeInfo = new PipelineInfo();
            procInfo = new ProcessInfo();
            stageInfo = new StageInfo<TDataIn, TDataOut>(pipeInfo, procInfo, this);
        }
        ~StageProcess() {}

        void SetPipelineInfo(PipelineInfo * _pipeInfo);
        void SetProcessInfo(ProcessInfo * _procInfo);

        void Init(int procJob);
        int Execute(int procJob);

        void InitSource();
        int ExecuteSource();
        int SelfAdaptiveSource(Ctx * ctx, int * bufConfig);

        void InitCompute();
        int ExecuteCompute();
        int SelfAdaptiveCompute(Ctx * ctx, int * bufConfig);

        void InitSink();
        int ExecuteSink();
        int SelfAdaptiveSink(Ctx * ctx, int * bufConfig);

        void SourceInternals(Ctx * ctx);

        // ### User interfaces
        virtual void OnInit(void * ctx) = 0;
        virtual void OnEnd(void * ctx) = 0;
        virtual void OnProduce(void * ctx) = 0;
        virtual void OnInput(void * ctx, void * body) = 0;

        void Produce(void * _ctx, void * _dataOut, int size){
            Ctx * ctx = static_cast<Ctx*>(_ctx);
            SourceInternals(ctx);
            if(pipeInfo->IsOrderingEnabled()) {
                ctx->GetHeader()->msgID = stageInfo->GetNextMsgID();
            }
            Publish(ctx, _dataOut, size);
        }

        void ProduceMulti(void * _ctx, vector<void *> dataOutList, vector<int> size){
            Ctx * ctx = static_cast<Ctx*>(_ctx);
            SourceInternals(ctx);
            ctx->GetHeader()->msgID = stageInfo->GetNextMsgID();
            PublishMulti(ctx, dataOutList, size);
        }

        void Publish(void * _ctx, void * dataOut, int size){
            Ctx * ctx = static_cast<Ctx*>(_ctx);
            MPI_Comm * outputComm = stageInfo->GetOutputComm();
            MPI_Request requestSend, requestDataSend;

            int targetRank = ctx->GetTargetProc();

            if(pipeInfo->IsOrderingEnabled()) {
                // configure header
                ctx->GetHeader()->incomingMsgs = 1;
                
                MPI_Isend(ctx->GetHeader(), sizeof(Header), MPI_BYTE, targetRank, HEADER_MSG, *outputComm, &requestSend);
                MPI_Wait(&requestSend, MPI_STATUS_IGNORE);
            }

            MPI_Isend(dataOut, size, MPI_BYTE, targetRank, DATA_MSG, *outputComm, &requestDataSend);
            MPI_Wait(&requestDataSend, MPI_STATUS_IGNORE);

            stageInfo->IncrementItemsProduced();
        };

        void PublishMulti(void * _ctx, vector<void *>& dataOutList, vector<int>& size){
            Ctx * ctx = static_cast<Ctx*>(_ctx);
            MPI_Comm * outputComm = stageInfo->GetOutputComm();
            MPI_Request requestSend, requestDataSend, requestDataSend2;

            if(dataOutList.size() != size.size()){
                throw std::runtime_error("The PublishMulti vectors must have the same size");
            } else if(dataOutList.size() > 5){
                throw std::runtime_error("The PublishMulti vector must have at most 5 elements (adjustable)");
            }

            // configure header
            ctx->GetHeader()->incomingMsgs = dataOutList.size();

            int targetRank = ctx->GetTargetProc();
            MPI_Isend(ctx->GetHeader(), sizeof(Header), MPI_BYTE, targetRank, HEADER_MSG, *outputComm, &requestSend);
            MPI_Wait(&requestSend, MPI_STATUS_IGNORE);

            MPI_Isend(dataOutList[0], size[0], MPI_BYTE, targetRank, DATA_MSG, *outputComm, &requestDataSend);
            MPI_Wait(&requestDataSend, MPI_STATUS_IGNORE);
            
            for(long unsigned int dataID=1; dataID<dataOutList.size(); dataID++){
                MPI_Isend(dataOutList[dataID], size[dataID], MPI_BYTE, targetRank, DATA_MSG, *outputComm, &requestDataSend2);
                MPI_Wait(&requestDataSend2, MPI_STATUS_IGNORE);
            }

            stageInfo->IncrementItemsProduced();
        };

        // ### Communication interfaces
        void * Pack(TDataOut _dataOut){
            TDataOut * dataOut = new TDataOut(_dataOut);
            return (void*)dataOut;
        };

        TDataIn Unpack(void * _dataIn) {
            TDataIn * dataIn = static_cast<TDataIn*>(_dataIn);
            return *dataIn;
        };

        // ### Optimization interfaces
        void EnableOrdering(){
            stageInfo->EnableOrdering();
        };

        bool IsOrderingEnabled(){
            return stageInfo->IsOrderingEnabled();
        };
    };

    template <typename TDataIn, typename TDataOut>
    void StageProcess<TDataIn, TDataOut>::Init(int procJob) {
        
        if (procJob == pipeInfo->GetNumStages()+1) {
            StageProcess::InitSource();
        }
        else if (procJob > pipeInfo->GetNumStages()+1 && procJob < pipeInfo->GetNumStages()*2) {
            StageProcess::InitCompute();
        }
        else if (procJob == pipeInfo->GetNumStages()*2) {
            StageProcess::InitSink();
        }
    }

    template <typename TDataIn, typename TDataOut>
    int StageProcess<TDataIn, TDataOut>::Execute(int procJob) {
        
        if (procJob == pipeInfo->GetNumStages()+1) {
            return StageProcess::ExecuteSource();
        }
        else if (procJob > pipeInfo->GetNumStages()+1 && procJob < pipeInfo->GetNumStages()*2) {
            return StageProcess::ExecuteCompute();
        }
        else if (procJob == pipeInfo->GetNumStages()*2) {
            return StageProcess::ExecuteSink();
        }
        return 0;
    }
  
    template <typename TDataIn, typename TDataOut>
    void StageProcess<TDataIn, TDataOut>::SetPipelineInfo(PipelineInfo * _pipeInfo){
        pipeInfo = _pipeInfo;
        stageInfo->SetPipelineInfo(_pipeInfo);
    }

    template <typename TDataIn, typename TDataOut>
    void StageProcess<TDataIn, TDataOut>::SetProcessInfo(ProcessInfo * _procInfo){
        procInfo = _procInfo;
        stageInfo->SetProcessInfo(_procInfo);
    }

    template <typename TDataIn, typename TDataOut>
    void StageProcess<TDataIn, TDataOut>::SourceInternals(Ctx * ctx) {
        do {
            waitingTime = std::chrono::duration<double>(std::chrono::steady_clock::now()-startClock).count();

            // Time to sync with stage manager
            if(waitingTime >= checkInterval){

                // code 1 - Process is alive
                stageStatus->code = 1;
                stageStatus->myRank = myLocalRank;
                stageStatus->itemsConsumed = 0;
                stageStatus->itemsProduced = stageInfo->GetItemsProduced();

                MPI_Isend(stageStatus, 1, serialization.MPI_STAGE_STATUS, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
                MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);

                // recv config code from stage manager
                MPI_Irecv(bufConfig, pipeInfo->GetNumStages()+1, MPI_INT, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
                MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);
            
                stageInfo->ResetItemsProduced();

                SelfAdaptiveSource(ctx, bufConfig);                
                startClock = std::chrono::steady_clock::now();
            }
            
            // --- on-demand mechanism 
            MPI_Improbe(MPI_ANY_SOURCE, REQUEST_MSG, *outputComm, &isMsgReady, &probeMsg, &status);
        } while (!isMsgReady);

        MPI_Imrecv(&emptyBuffer, 0, MPI_BYTE, &probeMsg, &requestOndemand);
        MPI_Wait(&requestOndemand, MPI_STATUS_IGNORE);
        
        ctx->SetTargetProc(status.MPI_SOURCE);
    }

    template <typename TDataIn, typename TDataOut>
    void StageProcess<TDataIn, TDataOut>::InitSource() {
        globalComm = pipeInfo->GetGlobalComm();
        pipelineComm = pipeInfo->GetPipelineComm();
        stageManagerComm = stageInfo->GetStageManagerComm();
        outputComm = stageInfo->GetOutputComm();
        
        MPI_Intercomm_create( *pipelineComm, 0, *globalComm, procInfo->GetStageID(), pipeInfo->GetStageManagerTag(procInfo->GetStageID()), stageManagerComm); 
        MPI_Intercomm_create( *pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(procInfo->GetStageID()+1), pipeInfo->GetOutputTag(procInfo->GetStageID()), outputComm); 

        if(procInfo->GetIsLateSpawnProcess()){ MPI_Barrier(*globalComm); }

        MPI_Comm_rank(*pipelineComm, &myLocalRank);

        bufConfig = new int[pipeInfo->GetNumStages()+1];
        
        endingPipeline = false;

        ctxBase.Init(pipeInfo->GetNumStages());
        stageStatus = ctxBase.GetStageStatus();
    }

    template <typename TDataIn, typename TDataOut>
    int StageProcess<TDataIn, TDataOut>::ExecuteSource() {
        startClock = std::chrono::steady_clock::now();

        OnInit(&ctxBase);

        OnProduce(&ctxBase);

        OnEnd(&ctxBase);

        // end of stream
        for (int workerRank = 0; workerRank < pipeInfo->GetStageSize(procInfo->GetStageID()+1); workerRank++) {
            do {
                MPI_Improbe(workerRank, REQUEST_MSG, *outputComm, &isMsgReady, &probeMsg, &status);
            } while (!isMsgReady);

            MPI_Imrecv(&emptyBuffer, 0, MPI_BYTE, &probeMsg, &requestOndemand);
            MPI_Wait(&requestOndemand, MPI_STATUS_IGNORE);

            MPI_Isend(&emptyBuffer, 0, MPI_BYTE, workerRank, STOP_MSG, *outputComm, &requestStop);
            MPI_Wait(&requestStop, MPI_STATUS_IGNORE);
        }  

        // send special eos to manager communicating the process has an intention to stop since there is no more data to process
        stageStatus->code = 8;
        stageStatus->myRank = myLocalRank;
        stageStatus->itemsConsumed = 0;
        stageStatus->itemsProduced = stageInfo->GetItemsProduced();
        MPI_Isend(stageStatus, 1, serialization.MPI_STAGE_STATUS, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
        MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);

        stageStatus->code = 1;
        while (true) {

            MPI_Isend(stageStatus, 1, serialization.MPI_STAGE_STATUS, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
            MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);

            // recv config code from stage manager
            MPI_Irecv(bufConfig, pipeInfo->GetNumStages()+1, MPI_INT, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
            MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);
            
            // code 7 - pipeline is ending
            if(bufConfig[0] == 7){ break; }

            SelfAdaptiveSource(&ctxBase, bufConfig);      
        }

        // send eos to manager
        stageStatus->code = 2;
        stageStatus->myRank = myLocalRank;
        stageStatus->itemsConsumed = 0;
        stageStatus->itemsProduced = stageInfo->GetItemsProduced();
        MPI_Isend(stageStatus, 1, serialization.MPI_STAGE_STATUS, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
        MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);

        MPI_Barrier(*globalComm);

        return 0;
    }

    template <typename TDataIn, typename TDataOut>
    int StageProcess<TDataIn, TDataOut>::SelfAdaptiveSource(Ctx * ctx, int * bufConfig) {
        // code 3 - synchronize to add processes to pipeline
        if(bufConfig[0] == 3) {
            // send synchronization marker to the output processes
            for (int workerRank = 0; workerRank < pipeInfo->GetStageSize(procInfo->GetStageID()+1); workerRank++) {
                MPI_Isend(&emptyBuffer, 0, MPI_BYTE, workerRank, READY_MSG, *outputComm, &requestConfig);
                MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);
            }  

            int amountNewProcesses = 0;

            // calculate the amount of new processes
            for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ amountNewProcesses += (bufConfig[stageID] - pipeInfo->GetStageSize(stageID)); }
            
            for(int processRank=1; processRank<=amountNewProcesses; processRank++){
                char ** argv = pipeInfo->GetArgv();

                MPI_Comm interComm;
                MPI_Comm_spawn(argv[0], argv+1, 1, MPI_INFO_NULL, 0, *globalComm, &interComm, MPI_ERRCODES_IGNORE);
                MPI_Intercomm_merge(interComm, 0, globalComm);
            }

            // Recv and update the process stage group ranks with the new processes added in the stage
            for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++) {
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
            }

            if(pipeInfo->GetStageSize(procInfo->GetStageID()) < bufConfig[procInfo->GetStageID()]) {
                pipeInfo->UpdateStageRanksUsingGlobalComm(procInfo->GetStageID());

                MPI_Group * stageProcessGroup = pipeInfo->GetStageProcessGroup(procInfo->GetStageID());

                int groupTag = 2000 + pipeInfo->GetNumStages() + 1 + procInfo->GetStageID(); 
                
                // create the group communicator
                MPI_Comm_create_group(*globalComm, *stageProcessGroup, groupTag, pipelineComm);
                MPI_Intercomm_create(*pipelineComm, 0, *globalComm, procInfo->GetStageID(), pipeInfo->GetStageManagerTag(procInfo->GetStageID()), stageManagerComm); 
            }
            
            // recreate the intercommunicator with the output processes
            MPI_Intercomm_create(*pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(procInfo->GetStageID()+1), pipeInfo->GetOutputTag(procInfo->GetStageID()), outputComm); 

            MPI_Barrier(*globalComm);

            // Update the stage sizes after the new processes are added
            pipeInfo->UpdateStageSize();

        } else if (bufConfig[0] == 4) {
            // send synchronization marker to the output processes
            for (int workerRank = 0; workerRank < pipeInfo->GetStageSize(procInfo->GetStageID()+1); workerRank++) {
                MPI_Isend(&emptyBuffer, 0, MPI_BYTE, workerRank, READY_MSG, *outputComm, &requestConfig);
                MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);
            }  
            
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

            // Obtain the group of processes in the world communicator
            MPI_Group newGlobalCommGroup;
            MPI_Group * globalGroup = pipeInfo->GetGlobalGroup();

            // Remove all banned ranks
            MPI_Group_excl(*globalGroup, banListSize, ranksToBan, &newGlobalCommGroup);

            // Replace the global communicator
            MPI_Comm_create(*globalComm, newGlobalCommGroup, globalComm);

            if(*globalComm != MPI_COMM_NULL) {
                // first step is updating all information
                pipeInfo->UpdateStageRanksUsingGlobalComm(procInfo->GetStageID());

                MPI_Group * stageProcessGroup = pipeInfo->GetStageProcessGroup(procInfo->GetStageID());

                int groupTag = 2000 + pipeInfo->GetNumStages() + 1 + procInfo->GetStageID(); 
                MPI_Comm_create_group(*globalComm, *stageProcessGroup, groupTag, pipelineComm);

                // update local rank after recreating the pipelineComm (it may changed)
                MPI_Comm_rank(*pipelineComm, &myLocalRank);
                
                // recreate all intercommunicators
                MPI_Intercomm_create(*pipelineComm, 0, *globalComm, procInfo->GetStageID(),pipeInfo->GetStageManagerTag(procInfo->GetStageID()), stageManagerComm); 
                MPI_Intercomm_create(*pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(procInfo->GetStageID()+1), pipeInfo->GetOutputTag(procInfo->GetStageID()), outputComm); 

                MPI_Barrier(*globalComm);
            }

            // Update the stage sizes after the new processes are added
            pipeInfo->UpdateStageSize();
        }
        return 0;
    }

    template <typename TDataIn, typename TDataOut>
    void StageProcess<TDataIn, TDataOut>::InitCompute() {
        globalComm = pipeInfo->GetGlobalComm();
        pipelineComm = pipeInfo->GetPipelineComm();
        stageManagerComm = stageInfo->GetStageManagerComm();
        inputComm = stageInfo->GetInputComm();
        outputComm = stageInfo->GetOutputComm();

        MPI_Intercomm_create( *pipelineComm, 0, *globalComm, procInfo->GetStageID(), pipeInfo->GetStageManagerTag(procInfo->GetStageID()), stageManagerComm); 
        MPI_Intercomm_create( *pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(procInfo->GetStageID()-1), pipeInfo->GetInputTag(procInfo->GetStageID()), inputComm); 
        MPI_Intercomm_create( *pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(procInfo->GetStageID()+1), pipeInfo->GetOutputTag(procInfo->GetStageID()), outputComm); 
        
        if(procInfo->GetIsLateSpawnProcess()){ MPI_Barrier(*globalComm); }

        MPI_Comm_rank(*pipelineComm, &myLocalRank);
    
        bufConfig = new int[pipeInfo->GetNumStages()+1];

        EOS = false;
        endingPipeline = false;

        ctxBase.Init(pipeInfo->GetNumStages());
        stageStatus = ctxBase.GetStageStatus();
        stageInfo->InitStopSignals();
    }


    template <typename TDataIn, typename TDataOut>
    int StageProcess<TDataIn, TDataOut>::ExecuteCompute() {
        startClock = std::chrono::steady_clock::now();

        OnInit(&ctxBase);

        MPI_Isend(&emptyBuffer, 0, MPI_BYTE, 0, REQUEST_MSG, *inputComm, &requestOndemand);
        MPI_Wait(&requestOndemand, MPI_STATUS_IGNORE);

        while (true) {

            do {
                waitingTime = std::chrono::duration<double>(std::chrono::steady_clock::now()-startClock).count();
                
                if(waitingTime >= checkInterval){
                    int localsize;
                    MPI_Comm_size(*pipelineComm, &localsize);
                    
                    // code 1 - Process is alive
                    stageStatus->code = 1;
                    stageStatus->myRank = myLocalRank;
                    stageStatus->itemsConsumed = stageInfo->GetItemsConsumed();
                    stageStatus->itemsProduced = stageInfo->GetItemsProduced();

                    MPI_Isend(stageStatus, 1, serialization.MPI_STAGE_STATUS, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
                    MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);
                    
                    // recv config code from stage manager
                    MPI_Irecv(bufConfig, pipeInfo->GetNumStages()+1, MPI_INT, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
                    MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);

                    stageInfo->ResetItemsConsumed();
                    stageInfo->ResetItemsProduced();

                    int ret = SelfAdaptiveCompute(&ctxBase, bufConfig);
                    if(ret == -1) { return -1; }
                    startClock = std::chrono::steady_clock::now();
                }
                
                MPI_Improbe(MPI_ANY_SOURCE, MPI_ANY_TAG, *inputComm, &isDataMsgReady, &probeMsg, &status);
            } while (!isDataMsgReady);

            ctxBase.SetMsgStatus(status);
            ctxBase.SetProbeMsg(probeMsg);

            EOS = stageInfo->ReceiveAndComputeMsg(&ctxBase);
            
            if(EOS) { 
                break; 
            }

            MPI_Isend(&emptyBuffer, 0, MPI_BYTE, 0, REQUEST_MSG, *inputComm, &requestOndemand);
            MPI_Wait(&requestOndemand, MPI_STATUS_IGNORE);
        } 
        
        ctxBase.SetTargetProc(0);
        OnEnd(&ctxBase);

        // end of stream
        for (int workerRank = 0; workerRank < pipeInfo->GetStageSize(procInfo->GetStageID()+1); workerRank++) {
            MPI_Isend(&emptyBuffer, 0, MPI_BYTE, workerRank, STOP_MSG, *outputComm, &requestStop);
            MPI_Wait(&requestStop, MPI_STATUS_IGNORE);
        }

        stageStatus->code = 1;
        stageStatus->myRank = myLocalRank;
        stageStatus->itemsConsumed = 0;
        stageStatus->itemsProduced = 0;
        while (true) {

            MPI_Isend(stageStatus, 1, serialization.MPI_STAGE_STATUS, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
            MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);

            // recv config code from stage manager
            MPI_Irecv(bufConfig, pipeInfo->GetNumStages()+1, MPI_INT, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
            MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);
            
            if(bufConfig[0] == 7){ break; }

            SelfAdaptiveCompute(&ctxBase, bufConfig);      
        }

        // send eos to manager
        stageStatus->code = 2;
        stageStatus->myRank = myLocalRank;
        stageStatus->itemsConsumed = stageInfo->GetItemsConsumed();
        stageStatus->itemsProduced = stageInfo->GetItemsProduced();
        MPI_Isend(stageStatus, 1, serialization.MPI_STAGE_STATUS, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
        MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);

        MPI_Barrier(*globalComm);

        return 0;
    }

    template <typename TDataIn, typename TDataOut>
    int StageProcess<TDataIn, TDataOut>::SelfAdaptiveCompute(Ctx * ctx, int * bufConfig) {
                            
        // code 3 - synchronize to add processes to pipeline
        if(bufConfig[0] == 3) {
            // check the buffer of important events searching for the READY_MSG synchronization marker
            vector<MPI_Status> * importantEvents = stageInfo->GetConfigurationEventsBuffer();
            bool readyMsgFound = false;
            for(long unsigned int event=0; event<importantEvents->size(); event++){
                if(importantEvents->at(event).MPI_TAG == READY_MSG){
                    importantEvents->erase(importantEvents->begin()+event);
                    readyMsgFound = true;
                    break;
                }
            }

            // process pending data until the previous stage notifies otherwise
            while(true){
                if(readyMsgFound) {break;}

                MPI_Improbe(MPI_ANY_SOURCE, MPI_ANY_TAG, *inputComm, &isDataMsgReady, &probeMsg, &status);

                if(isDataMsgReady){
                    // keep waiting until the previous stage sends the marker
                    if(status.MPI_TAG == READY_MSG){
                        MPI_Imrecv(&emptyBuffer, 0, MPI_BYTE, &probeMsg, &requestConfig);
                        MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);
                        break;
                    }

                    ctxBase.SetMsgStatus(status);
                    ctxBase.SetProbeMsg(probeMsg);
                    

                    EOS = stageInfo->ReceiveAndComputeMsg(&ctxBase);

                    if(EOS) { 
                        break; 
                    }
                }
            }

            // send synchronization marker to the output processes
            for (int workerRank = 0; workerRank < pipeInfo->GetStageSize(procInfo->GetStageID()+1); workerRank++) {
                MPI_Isend(&emptyBuffer, 0, MPI_BYTE, workerRank, READY_MSG, *outputComm, &requestConfig);
                MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);
            }  

            int amountNewProcesses = 0;
            // calculate the amount of new processes
            for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ 
                amountNewProcesses += (bufConfig[stageID] - pipeInfo->GetStageSize(stageID)); 
                // if previous stage added new processes, update the remaining stop signals
                if((bufConfig[stageID] - pipeInfo->GetStageSize(stageID)) != 0 && stageID == procInfo->GetStageID()-1){
                    stageInfo->AddStopSignals(bufConfig[stageID] - pipeInfo->GetStageSize(stageID));
                }
            }

            for(int processRank=1; processRank<=amountNewProcesses; processRank++){
                char ** argv = pipeInfo->GetArgv();

                MPI_Comm interComm;
                MPI_Comm_spawn(argv[0], argv+1, 1, MPI_INFO_NULL, 0, *globalComm, &interComm, MPI_ERRCODES_IGNORE);
                MPI_Intercomm_merge(interComm, 0, globalComm);
            }

            // Recv and update the process stage group ranks with the new processes added in the stage
            for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++) {
                do {
                    MPI_Improbe(0, RECONFIG_MSG, *globalComm, &isMsgReady, &probeMsgReconfig, &statusReconfig);
                } while (!isMsgReady);
                MPI_Get_count(&statusReconfig, MPI_INT, &numRanks);

                // The pipeline manager will send the new ranks of the processes added in the stage
                int newProcessStageRanks[numRanks];
                MPI_Imrecv(newProcessStageRanks, numRanks, MPI_INT, &probeMsgReconfig, &requestReconfig);
                MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);
                
                // Save the updated ranks in the corresponding stageID
                pipeInfo->SetProcessStageGroupRanks(stageID, newProcessStageRanks, numRanks);
            }

            if(pipeInfo->GetStageSize(procInfo->GetStageID()) < bufConfig[procInfo->GetStageID()]) {
                pipeInfo->UpdateStageRanksUsingGlobalComm(procInfo->GetStageID());
                
                MPI_Group * stageProcessGroup = pipeInfo->GetStageProcessGroup(procInfo->GetStageID());

                int groupTag = 2000 + pipeInfo->GetNumStages() + 1 + procInfo->GetStageID();

                // create the group communicator
                MPI_Comm_create_group(*globalComm, *stageProcessGroup, groupTag, pipelineComm);
                MPI_Intercomm_create(*pipelineComm, 0, *globalComm, procInfo->GetStageID(), pipeInfo->GetStageManagerTag(procInfo->GetStageID()), stageManagerComm); 
            }

            // recreate all intercommunicators
            MPI_Intercomm_create(*pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(procInfo->GetStageID()-1), pipeInfo->GetInputTag(procInfo->GetStageID()), inputComm); 
            MPI_Intercomm_create(*pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(procInfo->GetStageID()+1), pipeInfo->GetOutputTag(procInfo->GetStageID()), outputComm); 

            MPI_Barrier(*globalComm);

            // Update the stage sizes after the new processes are added
            pipeInfo->UpdateStageSize();
            
            // on-demand mechanism
            MPI_Isend(&emptyBuffer, 0, MPI_BYTE, 0, REQUEST_MSG, *inputComm, &requestOndemand);
            MPI_Wait(&requestOndemand, MPI_STATUS_IGNORE);
        } else if (bufConfig[0] == 4) {

            // check the buffer of important events searching for the READY_MSG synchronization marker
            vector<MPI_Status> * importantEvents = stageInfo->GetConfigurationEventsBuffer();
            bool readyMsgFound = false;
            for(long unsigned int event=0; event<importantEvents->size(); event++){
                
                if(importantEvents->at(event).MPI_TAG == READY_MSG){
                    importantEvents->erase(importantEvents->begin()+event);
                    readyMsgFound = true;
                    break;
                }
            }

            // check if there is pending data to process until the previous stage notifies otherwise
            while(true){
                if(readyMsgFound){ break; }

                MPI_Improbe(MPI_ANY_SOURCE, MPI_ANY_TAG, *inputComm, &isDataMsgReady, &probeMsg, &status);

                if(isDataMsgReady){
                    // keep waiting until the previous stage sends the marker
                    if(status.MPI_TAG == READY_MSG){
                        MPI_Imrecv(&emptyBuffer, 0, MPI_BYTE, &probeMsg, &requestConfig);
                        MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);
                        break;
                    }

                    ctxBase.SetMsgStatus(status);
                    ctxBase.SetProbeMsg(probeMsg);

                    EOS = stageInfo->ReceiveAndComputeMsg(&ctxBase);

                    if(EOS) {
                        break; 
                    }
                }
            }

            // send synchronization marker to the output processes
            for (int workerRank = 0; workerRank < pipeInfo->GetStageSize(procInfo->GetStageID()+1); workerRank++) {
                MPI_Request requestSend;
                MPI_Isend(&emptyBuffer, 0, MPI_BYTE, workerRank, READY_MSG, *outputComm, &requestSend);
                MPI_Wait(&requestSend, MPI_STATUS_IGNORE);
            }  

            do {
                MPI_Improbe(0, RECONFIG_MSG, *globalComm, &isMsgReady, &probeMsgReconfig, &statusReconfig);
            } while (!isMsgReady);
            MPI_Get_count(&statusReconfig, MPI_INT, &banListSize);


            // The pipeline manager will send the ranks of the processes chosen to be removed from the stage
            int ranksToBan[banListSize];
            MPI_Imrecv(ranksToBan, banListSize, MPI_INT, &probeMsgReconfig, &requestReconfig);
            MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);

            // update stopSignalsRemaining if a banned rank belongs to the previous stage
            for(int i = 0; i < banListSize; i++){
                if(pipeInfo->IsRankInStageProcessGroupRanks(procInfo->GetStageID()-1, ranksToBan[i])){
                    stageInfo->AckStopSignal();
                }
            }

            // recv and update the pipelineStageGroupsRanks
            for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++) {
                do {
                    MPI_Improbe(0, RECONFIG_MSG, *globalComm, &isMsgReady, &probeMsgReconfig, &statusReconfig);
                } while (!isMsgReady);
                MPI_Get_count(&statusReconfig, MPI_INT, &numRanks);
                // use an integer buffer to recv data
                int newProcessStageRanks[numRanks];
                MPI_Imrecv(newProcessStageRanks, numRanks, MPI_INT, &probeMsgReconfig, &requestReconfig);
                MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);

                // Clear and save the updated ranks in the corresponding stageID
                pipeInfo->ClearAndSetProcessStageGroupRanks(stageID, newProcessStageRanks, numRanks);
            }

            int myGlobalRank;
            MPI_Comm_rank(*globalComm, &myGlobalRank);

            // check if process is one of the banned compute processes
            // if so, he sends one last message to the output processes and then finishes
            for(int i = 0; i < banListSize; i++){
                if(ranksToBan[i] == myGlobalRank){
                    ctxBase.SetTargetProc(0);
                    OnEnd(&ctxBase);

                    MPI_Isend(&emptyBuffer, 0, MPI_BYTE, status.MPI_SOURCE, END_MSG, *outputComm, &requestOndemand);
                    MPI_Wait(&requestOndemand, MPI_STATUS_IGNORE);

                    // last moments of life: send one last message notifying the process is alive
                    stageStatus->code = 1;
                    stageStatus->myRank = myLocalRank;
                    stageStatus->itemsConsumed = stageInfo->GetItemsConsumed();
                    stageStatus->itemsProduced = stageInfo->GetItemsProduced();

                    MPI_Isend(stageStatus, 1, serialization.MPI_STAGE_STATUS, 0, BAN_MSG, *stageManagerComm, &requestConfig);
                    MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);


                    // Obtain the group of processes in the world communicator
                    MPI_Group newGlobalCommGroup;
                    MPI_Group * globalGroup = pipeInfo->GetGlobalGroup();

                    // Remove all banned ranks
                    MPI_Group_excl(*globalGroup, banListSize, ranksToBan, &newGlobalCommGroup);

                    // Help to recreate the global communicator
                    MPI_Comm_create(*globalComm, newGlobalCommGroup, globalComm);
                    
                    MPI_Finalize();
                    return -1;
                }
            }


            MPI_Isend(&emptyBuffer, 0, MPI_BYTE, status.MPI_SOURCE, END_MSG, *outputComm, &requestOndemand);
            MPI_Wait(&requestOndemand, MPI_STATUS_IGNORE);
                    
            // Obtain the group of processes in the world communicator
            MPI_Group newGlobalCommGroup;
            MPI_Group * globalGroup = pipeInfo->GetGlobalGroup();

            // Remove all banned ranks
            MPI_Group_excl(*globalGroup, banListSize, ranksToBan, &newGlobalCommGroup);
            
            // Replace the global communicator
            MPI_Comm_create(*globalComm, newGlobalCommGroup, globalComm);

            if(*globalComm != MPI_COMM_NULL) {

                pipeInfo->UpdateStageRanksUsingGlobalComm(procInfo->GetStageID());

                MPI_Group * stageProcessGroup = pipeInfo->GetStageProcessGroup(procInfo->GetStageID());

                int groupTag = 2000 + pipeInfo->GetNumStages() + 1 + procInfo->GetStageID();
                MPI_Comm_create_group(*globalComm, *stageProcessGroup, groupTag, pipelineComm);

                MPI_Comm_rank(*pipelineComm, &myLocalRank);

                // recreate all intercommunicators
                MPI_Intercomm_create(*pipelineComm, 0, *globalComm, procInfo->GetStageID(), pipeInfo->GetStageManagerTag(procInfo->GetStageID()), stageManagerComm); 
                MPI_Intercomm_create(*pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(procInfo->GetStageID()-1), pipeInfo->GetInputTag(procInfo->GetStageID()), inputComm); 
                MPI_Intercomm_create(*pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(procInfo->GetStageID()+1), pipeInfo->GetOutputTag(procInfo->GetStageID()), outputComm); 
                
                MPI_Barrier(*globalComm);
            }

            // Update the stage sizes after the new processes are added
            pipeInfo->UpdateStageSize();

            // on-demand mechanism
            MPI_Isend(&emptyBuffer, 0, MPI_BYTE, 0, REQUEST_MSG, *inputComm, &requestOndemand);
            MPI_Wait(&requestOndemand, MPI_STATUS_IGNORE);
        }
        return 0;
    }

    template <typename TDataIn, typename TDataOut>
    void StageProcess<TDataIn, TDataOut>::InitSink() {
        globalComm = pipeInfo->GetGlobalComm();
        pipelineComm = pipeInfo->GetPipelineComm();
        stageManagerComm = stageInfo->GetStageManagerComm();
        inputComm = stageInfo->GetInputComm();

        MPI_Intercomm_create( *pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(procInfo->GetStageID()-1), pipeInfo->GetInputTag(procInfo->GetStageID()), inputComm); 
        MPI_Intercomm_create( *pipelineComm, 0, *globalComm, procInfo->GetStageID(), /* STAGE3_MANAGER_PIPELINE */pipeInfo->GetStageManagerTag(procInfo->GetStageID()), stageManagerComm); 

        if(procInfo->GetIsLateSpawnProcess()){ MPI_Barrier(*globalComm); }

        MPI_Comm_rank(*pipelineComm, &myLocalRank);

        bufConfig = new int[pipeInfo->GetNumStages()+1];

        EOS = false;
        endingPipeline = false;
        sourceStageID = procInfo->GetStageID()-1;

        startClock = std::chrono::steady_clock::now();

        // reconfiguration mechanisms
        endMsgFound = new bool[pipeInfo->GetStageSize(sourceStageID)];
        for(int sourceRank=0; sourceRank<pipeInfo->GetStageSize(sourceStageID); sourceRank++){
            endMsgFound[sourceRank] = false;
        }
        
        ctxBase.Init(pipeInfo->GetNumStages());
        stageStatus = ctxBase.GetStageStatus();
        stageInfo->InitStopSignals();

    }

    template <typename TDataIn, typename TDataOut>
    int StageProcess<TDataIn, TDataOut>::ExecuteSink() {

        OnInit(&ctxBase);

        while (true) {  
            do {
                waitingTime = std::chrono::duration<double>(std::chrono::steady_clock::now()-startClock).count();

                if(waitingTime >= checkInterval){

                    // code 1 - Process is alive
                    stageStatus->code = 1;
                    stageStatus->myRank = myLocalRank;
                    stageStatus->itemsConsumed = stageInfo->GetItemsConsumed();
                    stageStatus->itemsProduced = 0;

                    MPI_Isend(stageStatus, 1, serialization.MPI_STAGE_STATUS, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
                    MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);

                    // recv config code from stage manager
                    MPI_Irecv(bufConfig, pipeInfo->GetNumStages()+1, MPI_INT, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
                    MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);

                    stageInfo->ResetItemsConsumed();

                    SelfAdaptiveSink(&ctxBase, bufConfig);

                    startClock = std::chrono::steady_clock::now();
                }

                MPI_Improbe(MPI_ANY_SOURCE, MPI_ANY_TAG, *inputComm, &isMsgReadyOndemandSolo, &probeMsgOndemandSolo, &statusOndemandSolo);
            } while (!isMsgReadyOndemandSolo);

            ctxBase.SetMsgStatus(statusOndemandSolo);
            ctxBase.SetProbeMsg(probeMsgOndemandSolo);

            EOS = stageInfo->ReceiveAndComputeMsg(&ctxBase);

            if(EOS) { break; }

        }

        OnEnd(&ctxBase);

        stageStatus->code = 1;
        stageStatus->myRank = myLocalRank;
        stageStatus->itemsConsumed = 0;
        stageStatus->itemsProduced = 0;
        while (true) {

            MPI_Isend(stageStatus, 1, serialization.MPI_STAGE_STATUS, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
            MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);

            // recv config code from stage manager
            MPI_Irecv(bufConfig, pipeInfo->GetNumStages()+1, MPI_INT, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
            MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);
            
            if(bufConfig[0] == 7){ break; }

            SelfAdaptiveSink(&ctxBase, bufConfig);      
        }

        // send eos to manager
        stageStatus->code = 2;
        stageStatus->myRank = myLocalRank;
        stageStatus->itemsConsumed = stageInfo->GetItemsConsumed();
        stageStatus->itemsProduced = 0;
        MPI_Isend(stageStatus, 1, serialization.MPI_STAGE_STATUS, 0, CONFIG_MSG, *stageManagerComm, &requestConfig);
        MPI_Wait(&requestConfig, MPI_STATUS_IGNORE);

        MPI_Barrier(*globalComm);

        return 0;
    }

    template <typename TDataIn, typename TDataOut>
    int StageProcess<TDataIn, TDataOut>::SelfAdaptiveSink(Ctx * ctx, int * bufConfig) {

        // code 3 - synchronize to add processes to pipeline
        if(bufConfig[0] == 3) {
            for(int sourceRank=0; sourceRank<pipeInfo->GetStageSize(sourceStageID); sourceRank++){
                endMsgFound[sourceRank] = false;
                // check the buffer of important events to see if any of them is the READY_MSG
                vector<MPI_Status> * importantEvents = stageInfo->GetConfigurationEventsBuffer();
                for(unsigned long int event=0; event<importantEvents->size(); event++){
                    
                    if(importantEvents->at(event).MPI_TAG == READY_MSG){
                        if(importantEvents->at(event).MPI_SOURCE == sourceRank){
                            importantEvents->erase(importantEvents->begin()+event);
                            endMsgFound[sourceRank] = true;
                            break;
                        }
                    }
                }
            }

            bool allEndMsgsFound = true;
            for(int sourceRank=0; sourceRank<pipeInfo->GetStageSize(sourceStageID); sourceRank++){
                allEndMsgsFound &= endMsgFound[sourceRank];
            }
            int sourceRank = 0;

            while(!allEndMsgsFound) {
                if(allEndMsgsFound){ break; }

                MPI_Improbe(MPI_ANY_SOURCE, MPI_ANY_TAG, *inputComm, &isMsgReadyOndemandSolo, &probeMsgOndemandSolo, &statusOndemandSolo);
                
                // keep waiting until the previous stage sends the marker
                if(isMsgReadyOndemandSolo) {
                    if(statusOndemandSolo.MPI_TAG == READY_MSG){
                        endMsgFound[statusOndemandSolo.MPI_SOURCE] = true;

                        allEndMsgsFound = true;
                        for(int sourceRank=0; sourceRank<pipeInfo->GetStageSize(sourceStageID); sourceRank++){
                            allEndMsgsFound &= endMsgFound[sourceRank]; 
                        }
                        if(allEndMsgsFound){ break; }
                        continue;
                    }

                    ctxBase.SetMsgStatus(statusOndemandSolo);
                    ctxBase.SetProbeMsg(probeMsgOndemandSolo);
                    
                    EOS = stageInfo->ReceiveAndComputeMsg(&ctxBase);
                }
            }

            // check if there is pending data to process for a "pendingDataCheckInterval" time 
            std::chrono::time_point<std::chrono::steady_clock> pendingDataStartClock = std::chrono::steady_clock::now();
            while(true){
                MPI_Improbe(MPI_ANY_SOURCE, MPI_ANY_TAG, *inputComm, &isMsgReadyOndemandSolo, &probeMsgOndemandSolo, &statusOndemandSolo);
                
                if(isMsgReadyOndemandSolo) {
                    ctxBase.SetMsgStatus(statusOndemandSolo);
                    ctxBase.SetProbeMsg(probeMsgOndemandSolo);
                    
                    EOS = stageInfo->ReceiveAndComputeMsg(&ctxBase);
                    if(EOS) { break; }
                } 

                double pendingDataWaitingTime = std::chrono::duration<double>(std::chrono::steady_clock::now()-pendingDataStartClock).count();

                // if the pendingDataCheckInterval time finishes, then move on
                if(pendingDataWaitingTime >= pendingDataCheckInterval){
                    break;
                }
            }

            int amountNewProcesses = 0;
            // calculate the amount of new processes
            for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++){ 
                amountNewProcesses += (bufConfig[stageID] - pipeInfo->GetStageSize(stageID)); 
                
                // if previous stage added new processes, update the remaining stop signals
                if((bufConfig[stageID] - pipeInfo->GetStageSize(stageID)) != 0 && stageID == sourceStageID){
                    // update stop signal
                    stageInfo->AddStopSignals((bufConfig[stageID] - pipeInfo->GetStageSize(stageID)));
                }
            }

            for(int processRank=1; processRank<=amountNewProcesses; processRank++){
                char ** argv = pipeInfo->GetArgv();

                MPI_Comm interComm;
                MPI_Comm_spawn(argv[0], argv+1, 1, MPI_INFO_NULL, 0, *globalComm, &interComm, MPI_ERRCODES_IGNORE);
                MPI_Intercomm_merge(interComm, 0, globalComm);
            }

            // Recv and update the process stage group ranks with the new processes added in the stage
            for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++) {
                do {
                    MPI_Improbe(0, RECONFIG_MSG, *globalComm, &isManagerMsgReady, &probeMsg, &status);
                } while (!isManagerMsgReady);
                MPI_Get_count(&status, MPI_INT, &numRanks);

                // The pipeline manager will send the new ranks of the processes added in the stage
                int newProcessStageRanks[numRanks];
                MPI_Imrecv(newProcessStageRanks, numRanks, MPI_INT, &probeMsg, &requestReconfig);
                MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);
                
                // Save the updated ranks in the corresponding stageID
                pipeInfo->SetProcessStageGroupRanks(stageID, newProcessStageRanks, numRanks);
            } 

            if(pipeInfo->GetStageSize(procInfo->GetStageID()) < bufConfig[procInfo->GetStageID()]) {
                pipeInfo->UpdateStageRanksUsingGlobalComm(procInfo->GetStageID());
                
                MPI_Group * stageProcessGroup = pipeInfo->GetStageProcessGroup(procInfo->GetStageID());
                
                int groupTag = 2000 + pipeInfo->GetNumStages() + 1 + procInfo->GetStageID(); 
                
                // create the group communicator
                MPI_Comm_create_group(*globalComm, *stageProcessGroup, groupTag, pipelineComm);
                MPI_Intercomm_create(*pipelineComm, 0, *globalComm, procInfo->GetStageID(), pipeInfo->GetStageManagerTag(procInfo->GetStageID()), stageManagerComm);
            }

            // recreate input intercommunicator
            MPI_Intercomm_create(*pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(sourceStageID), pipeInfo->GetInputTag(procInfo->GetStageID()), inputComm); 

            MPI_Barrier(*globalComm);


            // update local stage process ondemand management variables
            int newSize = bufConfig[sourceStageID];
            bool * endMsgFoundTemp = new bool[newSize];
            
            int oldSize = pipeInfo->GetStageSize(sourceStageID);
            memcpy(endMsgFoundTemp, endMsgFound, sizeof(bool)*oldSize);

            delete[] endMsgFound;

            endMsgFound = endMsgFoundTemp;

            for(int procRank = oldSize; procRank<newSize; procRank++){
                endMsgFound[procRank] = false;
            }

            // Update the stage sizes after the new processes are added
            pipeInfo->UpdateStageSize();
            
        } else if (bufConfig[0] == 4) {
            for(int sourceRank=0; sourceRank<pipeInfo->GetStageSize(sourceStageID); sourceRank++){
                endMsgFound[sourceRank] = false;
                // check the buffer of important events to see if any of them is the READY_MSG
                vector<MPI_Status> * importantEvents = stageInfo->GetConfigurationEventsBuffer();
                for(unsigned long int event=0; event<importantEvents->size(); event++){
                    
                    if(importantEvents->at(event).MPI_TAG == READY_MSG){
                        if(importantEvents->at(event).MPI_SOURCE == sourceRank){
                            importantEvents->erase(importantEvents->begin()+event);
                            endMsgFound[sourceRank] = true;
                            break;
                        }
                    }
                }
            }

            bool allEndMsgsFound = true;
            for(int sourceRank=0; sourceRank<pipeInfo->GetStageSize(sourceStageID); sourceRank++){
                allEndMsgsFound &= endMsgFound[sourceRank]; 
            }

            while(!allEndMsgsFound) {
                if(allEndMsgsFound){ break; }

                MPI_Improbe(MPI_ANY_SOURCE, MPI_ANY_TAG, *inputComm, &isMsgReadyOndemandSolo, &probeMsgOndemandSolo, &statusOndemandSolo);
                    
                if(isMsgReadyOndemandSolo) {
                    if(statusOndemandSolo.MPI_TAG == READY_MSG){
                        endMsgFound[statusOndemandSolo.MPI_SOURCE] = true;

                        allEndMsgsFound = true;
                        for(int sourceRank=0; sourceRank<pipeInfo->GetStageSize(sourceStageID); sourceRank++){
                            allEndMsgsFound &= endMsgFound[sourceRank]; 
                        }
                        if(allEndMsgsFound){ break; }
                        continue;
                    }

                    ctxBase.SetMsgStatus(statusOndemandSolo);
                    ctxBase.SetProbeMsg(probeMsgOndemandSolo);
                    
                    EOS = stageInfo->ReceiveAndComputeMsg(&ctxBase);
                }
            }

            // recv the ranksToBan list
            do {
                MPI_Improbe(0, RECONFIG_MSG, *globalComm, &isMsgReady, &probeMsg, &status);
            } while (!isMsgReady);
            MPI_Get_count(&status, MPI_INT, &banListSize);

            // use an integer buffer to recv data
            int ranksToBan[banListSize];
            MPI_Imrecv(ranksToBan, banListSize, MPI_INT, &probeMsg, &requestReconfig);
            MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);

            // update stopSignalsRemaining if a banned rank belongs to the previous stage
            for(int i = 0; i < banListSize; i++){
                if(pipeInfo->IsRankInStageProcessGroupRanks(sourceStageID, ranksToBan[i])){
                    stageInfo->AckStopSignal();
                }
            }

            // Recv and update the process stage group ranks with the new ranks send by the pipeline manager
            for(int stageID = 1; stageID <= pipeInfo->GetNumStages(); stageID++) {
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

            // wait for the END_MSG messages to recreate the communicators
            for(int sourceRank=0; sourceRank<pipeInfo->GetStageSize(sourceStageID); sourceRank++){
                MPI_Isend(&emptyBuffer, 0, MPI_BYTE, sourceRank, REQUEST_MSG, *inputComm, &requestOndemandSolo);
                MPI_Wait(&requestOndemandSolo, MPI_STATUS_IGNORE);

                // check the buffer of important events to see if any of them is the END_MSG
                vector<MPI_Status> * importantEvents = stageInfo->GetConfigurationEventsBuffer();
                bool endMsgFound = false;
                for(long unsigned int event=0; event<importantEvents->size(); event++){
                    
                    if(importantEvents->at(event).MPI_TAG == END_MSG){
                        if(importantEvents->at(event).MPI_SOURCE == sourceRank){
                            importantEvents->erase(importantEvents->begin()+event);
                            endMsgFound = true;
                            break;
                        }
                    }
                }
                if(endMsgFound){ continue; }

                while(true){
                    MPI_Improbe(MPI_ANY_SOURCE, MPI_ANY_TAG, *inputComm, &isMsgReadyOndemandSolo, &probeMsgOndemandSolo, &statusOndemandSolo);

                    if(isMsgReadyOndemandSolo) {
                        if(statusOndemandSolo.MPI_TAG == END_MSG){
                            break;
                        }

                        ctxBase.SetMsgStatus(statusOndemandSolo);
                        ctxBase.SetProbeMsg(probeMsgOndemandSolo);

                        EOS = stageInfo->ReceiveAndComputeMsg(&ctxBase);
                        if(EOS) { break; }
                        
                        MPI_Isend(&emptyBuffer, 0, MPI_BYTE, statusOndemandSolo.MPI_SOURCE, REQUEST_MSG, *inputComm, &requestOndemandSolo);
                        MPI_Wait(&requestOndemandSolo, MPI_STATUS_IGNORE);
                    }
                }
            }

            // Obtain the group of processes in the world communicator
            MPI_Group globalCommGroup, newGlobalCommGroup;
            MPI_Comm_group(*globalComm, &globalCommGroup);

            // Remove all banned ranks
            MPI_Group_excl(globalCommGroup, banListSize, ranksToBan, &newGlobalCommGroup);

            // Replace the global communicator
            MPI_Comm_create(*globalComm, newGlobalCommGroup, globalComm);

            if(*globalComm != MPI_COMM_NULL) {

                pipeInfo->UpdateStageRanksUsingGlobalComm(procInfo->GetStageID());

                MPI_Group * stageProcessGroup = pipeInfo->GetStageProcessGroup(procInfo->GetStageID());

                int groupTag = 2000 + pipeInfo->GetNumStages() + 1 + procInfo->GetStageID(); 
                MPI_Comm_create_group(*globalComm, *stageProcessGroup, groupTag, pipelineComm);
                
                MPI_Comm_rank(*pipelineComm, &myLocalRank);

                // recreate all intercommunicators
                MPI_Intercomm_create(*pipelineComm, 0, *globalComm, procInfo->GetStageID(), pipeInfo->GetStageManagerTag(procInfo->GetStageID()), stageManagerComm);
                MPI_Intercomm_create(*pipelineComm, 0, *globalComm, pipeInfo->GetStageFirstRank(sourceStageID), pipeInfo->GetInputTag(procInfo->GetStageID()), inputComm); 
    
                MPI_Barrier(*globalComm);
            }

            // Update the stage sizes after the new processes are added
            pipeInfo->UpdateStageSize();

            // update local stage process ondemand management variables
            bool * endMsgFoundTemp = new bool[pipeInfo->GetStageSize(sourceStageID)];
            memcpy(endMsgFoundTemp, endMsgFound, sizeof(bool)*pipeInfo->GetStageSize(sourceStageID));
            delete[] endMsgFound;
            endMsgFound = endMsgFoundTemp;
        }
        return 0;

    }
}