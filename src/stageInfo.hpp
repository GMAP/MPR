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

#include <vector>
#include <queue>

#include "structs.hpp"
#include "stageProcess.hpp"
#include "stageProcessCtx.hpp"
#include "pipelineInfo.hpp"
#include "processInfo.hpp"
namespace mpr
{
    template <typename TDataIn, typename TDataOut>
    class StageInfo { 
    private:
        PipelineInfo * pipeInfo;
        ProcessInfo * procInfo;
        StageProcess<TDataIn, TDataOut> * stageProc;

        MPI_Comm stageManagerComm;
        MPI_Comm inputComm;
        MPI_Comm outputComm;

        char emptyBuffer;
        
        int itemsConsumed = 0;
        int itemsProduced = 0;

        // ordering mechanisms
        std::priority_queue<StreamItem<TDataIn>,std::deque<StreamItem<TDataIn>>,StreamItemComparison<TDataIn>> orderingBuffer; 
        long msgID = 0;
        long expectedMsgID = 1; 
        bool orderingEnabled = false;

        // End Of Stream (EOS) mechanisms
        int stopSignalsRemaining;

        // this buffer holds important runtime events that were captured by mistake
        vector<MPI_Status> ConfigurationEventsBuffer;

        // these buffers holds the ranks and probes of the processes that requested data
        queue<int> requestRanksBuffer;
        queue<MPI_Message*> requestProbesBuffer;

        // MPI
        int size;
        int incomingMsgs;
        int isDataMsgReady;
        MPI_Status status;
        MPI_Message probeMsg;
        MPI_Request requestRecv, requestRecvData, requestRecvStop;
        MPI_Message probeDataMsg;
        MPI_Status statusRecv;

        // Data buffers
        Header header;
        TDataIn payload;

        char ** buffer = new char*[5];
        int * bufferSize = new int[5];

    public:
        StageInfo(PipelineInfo * _pipeInfo, ProcessInfo * _procInfo, StageProcess<TDataIn, TDataOut> * _stageProc): pipeInfo(_pipeInfo), procInfo(_procInfo), stageProc(_stageProc) {
            // up to 5 multiple messages are allowed
            for(int i=0; i<5; i++){
                buffer[i] = new char[1];
                bufferSize[i] = 1;
            }
        }
        
        ~StageInfo() { }

        void SetPipelineInfo(PipelineInfo * _pipeInfo){ pipeInfo = _pipeInfo; }
        void SetProcessInfo(ProcessInfo * _procInfo){ procInfo = _procInfo; }

        MPI_Comm * GetStageManagerComm() { return &stageManagerComm; }
        MPI_Comm * GetInputComm() { return &inputComm; }
        MPI_Comm * GetOutputComm() { return &outputComm; }

        void IncrementItemsConsumed() { itemsConsumed++; }
        void IncrementItemsProduced() { itemsProduced++; }

        int GetItemsConsumed() { return itemsConsumed; }
        int GetItemsProduced() { return itemsProduced; }

        void ResetItemsConsumed() { itemsConsumed = 0; }
        void ResetItemsProduced() { itemsProduced = 0; }

        int GetNextMsgID() {
            msgID++;
            if(msgID == LONG_MAX) { msgID = 0; }
            return msgID;
        }
        int GetExpectedMsgID() { return expectedMsgID; }
        void IncrementExpectedMsgID() { 
            expectedMsgID++; 
            if(expectedMsgID == LONG_MAX) { expectedMsgID = 0; }
        }

        void AddToOrderingBuffer(StreamItem<TDataIn> item) { orderingBuffer.push(item); }
        StreamItem<TDataIn> GetTopFromOrderingBuffer() { return orderingBuffer.top(); }
        void RemoveFromOrderingBuffer() { orderingBuffer.pop(); }
        bool IsOrderingBufferEmpty() { return orderingBuffer.empty(); }

        void EnableOrdering() { orderingEnabled = true; }
        bool IsOrderingEnabled() { return orderingEnabled; }

        void InitStopSignals() { stopSignalsRemaining = pipeInfo->GetStageSize(procInfo->GetStageID()-1); }
        void AddStopSignals(int amountNewStopSignals) { stopSignalsRemaining += amountNewStopSignals; }
        void AckStopSignal() { stopSignalsRemaining--; }
        bool IsEndOfStream() { return stopSignalsRemaining == 0; }

        vector<MPI_Status> * GetConfigurationEventsBuffer() { return &ConfigurationEventsBuffer; }

        bool ReceiveAndComputeMsg(Ctx * ctx) {
            status = ctx->GetMsgStatus();
            probeMsg = ctx->GetProbeMsg();
            if (status.MPI_TAG == HEADER_MSG){

                MPI_Imrecv(&header, sizeof(Header), MPI_BYTE, &probeMsg, &requestRecvData);
                MPI_Wait(&requestRecvData, MPI_STATUS_IGNORE);

                if(IsOrderingEnabled()){
                    bool isMsgFromOrderingQueue = false;
                    while(1){
                        if(header.msgID != GetExpectedMsgID()){
                            //************************************************
                            // UNEXPECTED MSG, PUT IT IN THE ORDERING BUFFER
                            //************************************************
                            incomingMsgs = header.incomingMsgs-1;
                            
                            StreamItem<TDataIn> streamItemOrdering;
                            streamItemOrdering.header = header;

                            do{
                                MPI_Improbe(status.MPI_SOURCE, DATA_MSG, inputComm, &isDataMsgReady, &probeDataMsg, &statusRecv);
                            }while(!isDataMsgReady);

                            MPI_Imrecv(&streamItemOrdering.payload, sizeof(TDataIn), MPI_BYTE, &probeDataMsg, &requestRecvData);
                            MPI_Wait(&requestRecvData, MPI_STATUS_IGNORE);

                            while(incomingMsgs > 0){
                                do{
                                    MPI_Improbe(status.MPI_SOURCE, DATA_MSG, inputComm, &isDataMsgReady, &probeDataMsg, &statusRecv);
                                }while(!isDataMsgReady);

                                MPI_Get_count(&statusRecv, MPI_BYTE, &size);

                                // buffers are already used, create new ones
                                if (bufferSize[incomingMsgs-1] == -1) {
                                    buffer[incomingMsgs-1] = new char[size];
                                    bufferSize[incomingMsgs-1] = size;
                                } 
                                // if the size changes, clean it and use the new size
                                // the size cannot be bigger, but it can be smaller by 10%
                                else if(size > bufferSize[incomingMsgs-1] || size < bufferSize[incomingMsgs-1]*0.9) {
                                    delete[] buffer[incomingMsgs-1];
                                    buffer[incomingMsgs-1] = new char[size];
                                    bufferSize[incomingMsgs-1] = size;
                                }
                                
                                MPI_Imrecv(buffer[incomingMsgs-1], size, MPI_BYTE, &probeDataMsg, &requestRecvData);
                                MPI_Wait(&requestRecvData, MPI_STATUS_IGNORE);
                                // find the first pointer after the (static data + offset) and assign the buffer to it
                                // Pointers are always appended to the end of the static data
                                // e.g., pointer + sizeof(data) - offset
                                // *payload + sizeof(TDataOut)/8 - 1 = 0x1000 + (64/8) - 1 = 0x1007
                                *((char**)&streamItemOrdering.payload+(sizeof(TDataIn)/8)-incomingMsgs) = buffer[incomingMsgs-1];
                                incomingMsgs--;
                            }

                            AddToOrderingBuffer(streamItemOrdering);
                                
                            // since the buffer is utilized, they cannot be reused
                            // mark them as unused with -1
                            incomingMsgs = header.incomingMsgs-1;
                            while(incomingMsgs > 0){
                                bufferSize[incomingMsgs-1] = -1;
                                incomingMsgs--;
                            }

                            break;
                        }
                        //************************************************
                        // EXPECTED MSG, PROCESS IT
                        //************************************************
                        if(!isMsgFromOrderingQueue) {
                            incomingMsgs = header.incomingMsgs-1;

                            do{
                                MPI_Improbe(status.MPI_SOURCE, DATA_MSG, inputComm, &isDataMsgReady, &probeDataMsg, &statusRecv);
                            }while(!isDataMsgReady);
                            
                            MPI_Imrecv(&payload, sizeof(TDataIn), MPI_BYTE, &probeDataMsg, &requestRecvData);
                            MPI_Wait(&requestRecvData, MPI_STATUS_IGNORE);

                            while(incomingMsgs > 0){
                                do{
                                    MPI_Improbe(status.MPI_SOURCE, DATA_MSG, inputComm, &isDataMsgReady, &probeDataMsg, &statusRecv);
                                }while(!isDataMsgReady);

                                MPI_Get_count(&statusRecv, MPI_BYTE, &size);

                                // buffers are already used, create new ones
                                if (bufferSize[incomingMsgs-1] == -1) {
                                    buffer[incomingMsgs-1] = new char[size];
                                    bufferSize[incomingMsgs-1] = size;
                                } 
                                // if the size changes, clean it and use the new size
                                // the size cannot be bigger, but it can be smaller by 10%
                                else if(size > bufferSize[incomingMsgs-1] || size < bufferSize[incomingMsgs-1]*0.9) {
                                    delete[] buffer[incomingMsgs-1];
                                    buffer[incomingMsgs-1] = new char[size];
                                    bufferSize[incomingMsgs-1] = size;

                                }
                                
                                MPI_Imrecv(buffer[incomingMsgs-1], size, MPI_BYTE, &probeDataMsg, &requestRecvData);
                                MPI_Wait(&requestRecvData, MPI_STATUS_IGNORE);
                                // find the first pointer after the (static data + offset) and assign the buffer to it
                                // Pointers are always appended to the end of the static data
                                // e.g., pointer + sizeof(data) - offset
                                // *payload + sizeof(TDataOut)/8 - 1 = 0x1000 + (64/8) - 1 = 0x1007
                                *((char**)&payload+(sizeof(TDataIn)/8)-incomingMsgs) = buffer[incomingMsgs-1];
                                incomingMsgs--;
                            }
                        }

                        IncrementItemsConsumed();
                        
                        ctx->SetTargetProc(0);
                        ctx->SetHeader(&header);
                        stageProc->OnInput(ctx, (void*)&payload);
                        IncrementExpectedMsgID();


                        if(isMsgFromOrderingQueue){
                            // freeing memory
                            incomingMsgs = header.incomingMsgs-1;
                            while(incomingMsgs > 0){
                                delete[] *((char**)&payload+(sizeof(TDataIn)/8)-(incomingMsgs));
                                incomingMsgs--;
                            }
                            RemoveFromOrderingBuffer();
                        }

                        if(IsOrderingBufferEmpty() || (GetExpectedMsgID() < GetTopFromOrderingBuffer().header.msgID ) ) {
                            break; 
                        }

                        StreamItem<TDataIn> streamItem = GetTopFromOrderingBuffer();
                        header = streamItem.header;
                        payload = streamItem.payload;
                        isMsgFromOrderingQueue = true;
                    }
                } else {
                    incomingMsgs = header.incomingMsgs-1;

                    do{
                        MPI_Improbe(status.MPI_SOURCE, DATA_MSG, inputComm, &isDataMsgReady, &probeDataMsg, &statusRecv);
                    }while(!isDataMsgReady);

                    MPI_Imrecv(&payload, sizeof(TDataIn), MPI_BYTE, &probeDataMsg, &requestRecvData);
                    MPI_Wait(&requestRecvData, MPI_STATUS_IGNORE);

                    while(incomingMsgs > 0){
                        do{
                            MPI_Improbe(status.MPI_SOURCE, DATA_MSG, inputComm, &isDataMsgReady, &probeDataMsg, &statusRecv);
                        }while(!isDataMsgReady);

                        MPI_Get_count(&statusRecv, MPI_BYTE, &size);

                        if(size > bufferSize[incomingMsgs-1] || size < bufferSize[incomingMsgs-1]*0.9) {
                            delete[] buffer[incomingMsgs-1];
                            buffer[incomingMsgs-1] = new char[size];
                            bufferSize[incomingMsgs-1] = size;

                        }
                        
                        MPI_Imrecv(buffer[incomingMsgs-1], size, MPI_BYTE, &probeDataMsg, &requestRecvData);
                        MPI_Wait(&requestRecvData, MPI_STATUS_IGNORE);

                        *((char**)&payload+(sizeof(TDataIn)/8)-incomingMsgs) = buffer[incomingMsgs-1];
                        incomingMsgs--;
                    }

                    IncrementItemsConsumed();

                    ctx->SetTargetProc(0);
                    ctx->SetHeader(&header);
                    stageProc->OnInput(ctx, (void*)&payload);
                }
            }
            else if (status.MPI_TAG == STOP_MSG) {
                MPI_Imrecv(&emptyBuffer, 0, MPI_BYTE, &probeMsg, &requestRecvStop);
                MPI_Wait(&requestRecvStop, MPI_STATUS_IGNORE);

                AckStopSignal();

                if (IsEndOfStream()) {
                    return true;
                }
            } 
            else if (status.MPI_TAG == DATA_MSG){
                MPI_Imrecv(&payload, sizeof(TDataIn), MPI_BYTE, &probeMsg, &requestRecvData);
                MPI_Wait(&requestRecvData, MPI_STATUS_IGNORE);

                IncrementItemsConsumed();

                // compute message
                ctx->SetTargetProc(0);
                stageProc->OnInput(ctx, (void*)&payload);
            }
            else if(status.MPI_TAG == END_MSG){
                ConfigurationEventsBuffer.push_back(status);
            } 
            else if(status.MPI_TAG == READY_MSG){
                ConfigurationEventsBuffer.push_back(status);
            }
            return false;
        }
    };
}