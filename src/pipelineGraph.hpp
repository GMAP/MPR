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

#include <vector>

// Internal files
#include "pipelineInfo.hpp"
#include "processInfo.hpp"
#include "stageInfo.hpp"

#include "pipelineManager.hpp"
#include "stageManager.hpp"
#include "stageProcess.hpp"

#include "structs.hpp"
#include "defines.hpp"
#include "stageProcessCtx.hpp"

using namespace std;
using namespace nlohmann;

namespace mpr{

    class PipelineGraph
    {
    private:
        int * bufConfig;
        int * bufConfigAux;
        
        PipelineInfo pipeInfo;
        ProcessInfo procInfo;
        std::vector<Operator *> stageList;
        PipelineManager * pipeManager;
        StageManager * stageManager;

        void SpawnProcs();
        void SetJobs();
        void Init();
        int Execute();
        
    public:
        PipelineGraph(int *argc, char ***argv, int numberOfStages): pipeInfo(*argv, numberOfStages), procInfo() {
            bufConfig = new int[numberOfStages+1];
            bufConfigAux = new int[numberOfStages+1];
      
            MPI_Init(argc, argv);
        }
        ~PipelineGraph() {}

        void Run();

        void AddPipelineStage(Operator * stageProcess);
    };

    void PipelineGraph::Run(){
        MPI_Comm * globalComm = pipeInfo.GetGlobalComm();

        std::chrono::time_point<std::chrono::steady_clock> startInitalization;
        std::chrono::time_point<std::chrono::steady_clock> startComputation;
        std::chrono::time_point<std::chrono::steady_clock> endComputation;

        // the pipeline manager measures the execution time since it is the last process to leave
        if(procInfo.GetMyGlobalRank(MPI_COMM_WORLD) == 0) {
            startInitalization = std::chrono::steady_clock::now();
        }

        // dynamically spawn the processes
        PipelineGraph::SpawnProcs();

        // assign new jobs for spawned processes
        PipelineGraph::SetJobs();

        // now that processes know what to compute, initialize them
        PipelineGraph::Init();

        if(procInfo.GetMyGlobalRank(*globalComm) == 0) {
            startComputation = std::chrono::steady_clock::now();
        }
        
        // after initialization, start the processes' computation
        int ret = PipelineGraph::Execute();

        if (ret == -1) { return; }
        
        if(procInfo.GetMyGlobalRank(*globalComm) == 0) {
            endComputation = std::chrono::steady_clock::now();
        }

        // Execution time statistics
        if (procInfo.GetMyGlobalRank(*globalComm) == 0) {
            std::chrono::duration< double > initializationTime = startComputation - startInitalization;
            std::chrono::duration< double > computationTime = endComputation - startComputation;

            std::chrono::milliseconds initializationMillisec = std::chrono::duration_cast< std::chrono::milliseconds >( initializationTime );
            std::chrono::milliseconds computationMillisec = std::chrono::duration_cast< std::chrono::milliseconds >( computationTime );

            std::cout << endl << "--- Pipeline Execution Statistics ---" << std::endl;
            std::cout << "Number of processes: " << pipeInfo.GetAllStagesSize() << std::endl;
            std::cout << "Execution time: " << computationMillisec.count() << " ms." << std::endl;
        }

        MPI_Barrier(*globalComm);
        MPI_Finalize();
    }

    void PipelineGraph::Init(){
        // initialize the pipeline manager
        if (procInfo.GetProcJob() == 0){
            pipeManager = new PipelineManager(&pipeInfo, &procInfo);
        } 
        // initialize the stage managers
        else if (procInfo.GetProcJob() >= 0 && procInfo.GetProcJob() <= pipeInfo.GetNumStages()){
            stageManager = new StageManager(&pipeInfo, &procInfo, procInfo.GetProcJob());
        } 
        // initialize the stage processes (Source, Compute, Sink)
        else if (procInfo.GetProcJob() >= pipeInfo.GetNumStages()+1){
            for(int i = 0; i < pipeInfo.GetNumStages(); i++){
                if(stageList[i]->IsOrderingEnabled()){
                    pipeInfo.EnableOrdering();
                }
            }
            int job = procInfo.GetProcJob()-pipeInfo.GetNumStages()-1;

            stageList[job]->SetPipelineInfo(&pipeInfo);
            stageList[job]->SetProcessInfo(&procInfo);

            stageList[job]->Init(procInfo.GetProcJob());
        }
    }

    int PipelineGraph::Execute(){
        if (procInfo.GetProcJob() == 0){
            pipeManager->Execute();
        } 
        else if (procInfo.GetProcJob() >= 0 && procInfo.GetProcJob() <= pipeInfo.GetNumStages()){
            stageManager->Execute(procInfo.GetProcJob());
        } 
        else if (procInfo.GetProcJob() >= pipeInfo.GetNumStages()+1){
            return stageList[procInfo.GetProcJob()-pipeInfo.GetNumStages()-1]->Execute(procInfo.GetProcJob());
        }
        return 0;
    }
    
    void PipelineGraph::SpawnProcs(){
        MPI_Comm parentComm;
        MPI_Comm_get_parent(&parentComm);
        MPI_Comm * globalComm = pipeInfo.GetGlobalComm();
        char ** argv = pipeInfo.GetArgv();

        // only the pipeline manager enters this block
        if(parentComm == MPI_COMM_NULL){
            // n_procs contains the total number of processes that will be spawned
            int n_procs = pipeInfo.GetNumStages(); // increment by one manager process for each stage
            for(int stageID = 1; stageID <= pipeInfo.GetNumStages(); stageID++) {
                n_procs += pipeInfo.GetStageSize(stageID); // increment by the number of working processes in each stage
            }
            
            MPI_Comm interComm;
            MPI_Comm_spawn(argv[0], argv+1, 1, MPI_INFO_NULL, 0, MPI_COMM_WORLD, &interComm, MPI_ERRCODES_IGNORE);
            MPI_Intercomm_merge(interComm, 0, globalComm);

            // Spawn new MPI processes
            for(int processRank=1; processRank<=n_procs; processRank++){
                bufConfigAux[0] = 6;
                bufConfigAux[1] = n_procs-processRank;
                
                MPI_Request requestReconfig;
                MPI_Isend(bufConfigAux, pipeInfo.GetNumStages()+1, MPI_INT, processRank, RECONFIG_MSG, *globalComm, &requestReconfig);
                MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);

                if(processRank == n_procs) break;

                MPI_Comm_spawn(argv[0], argv+1, 1, MPI_INFO_NULL, 0, *globalComm, &interComm, MPI_ERRCODES_IGNORE);
                MPI_Intercomm_merge(interComm, 0, globalComm);
            }
            
            // send the initialization config code to the new processes
            for(int processRank=1; processRank<=n_procs; processRank++){

                // initialize new spawned processes with the default configuration (code 0)
                // this is crucial because initially including them in the pipeline is straightforward
                // during application execution, all newly spawned processes will run a special routine to learn about the pipeline configuration
                bufConfig[0] = 0;

                MPI_Request requestReconfig;
                MPI_Isend(bufConfig, pipeInfo.GetNumStages()+1, MPI_INT, processRank, RECONFIG_MSG, *globalComm, &requestReconfig);
                MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);
            }
        }
        // all processes other than the pipeline manager enter this block
        else{
            MPI_Intercomm_merge(parentComm, 1, globalComm);

            MPI_Request requestReconfig;
            MPI_Irecv(bufConfigAux, pipeInfo.GetNumStages()+1, MPI_INT, 0, RECONFIG_MSG, *globalComm, &requestReconfig);
            MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);

            // all processes must help to spawn new processes
            MPI_Comm interComm;
            for(int processRank=1; processRank<=bufConfigAux[1]; processRank++){
                MPI_Comm_spawn(argv[0], argv+1, 1, MPI_INFO_NULL, 0, *globalComm, &interComm, MPI_ERRCODES_IGNORE);
                MPI_Intercomm_merge(interComm, 0, globalComm);
            }

            // recv config msg from pipeline manager
            // if recv config code 0, execute the default initialization
            // if recv config code 5, initialize using the state sent by the pipeline manager
            // this is checked in the SetJobs() function
            MPI_Irecv(bufConfig, pipeInfo.GetNumStages()+1, MPI_INT, 0, RECONFIG_MSG, *globalComm, &requestReconfig);
            MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);
        }
    }

    void PipelineGraph::SetJobs(){
        MPI_Comm * globalComm = pipeInfo.GetGlobalComm();

        int globalSize = pipeInfo.GetPipelineSize();
        pipeInfo.SetPipelineOriginalSize(globalSize);

        // default initialization
        if(bufConfig[0] == 0) {
            procInfo.SetIsLateSpawnProcess(false);
            
            // Pipeline Manager Groups
            pipeInfo.SetStageManagerGroups();

            // Pipeline Stage Groups
            pipeInfo.SetStageProcessGroups();

            // now that groups are set, check which processes belong to which group and create their respective communicators
            // first check if the process was chosen to be the pipeline manager
            for(int managerID = 0; managerID <= pipeInfo.GetNumStages(); managerID++) {
                // check if process belongs to group
                int localRank;
                MPI_Group * stageManagerGroup = pipeInfo.GetStageManagerGroup(managerID);
                MPI_Group_rank(*stageManagerGroup, &localRank);
                
                // if the process belongs to the group, create the communicator
                if(localRank != MPI_UNDEFINED) { 
                    // each Manager has its stage's corresponding id as the MPI rank
                    // e.g., manager 1 has rank 1 in the pipeline
                    procInfo.SetProcJob(managerID);

                    int groupTag = 2000+managerID; 
                    int rank;
                    MPI_Comm_rank(*globalComm, &rank);

                    // create the group communicator
                    MPI_Comm * pipelineComm = pipeInfo.GetPipelineComm();
                    MPI_Comm_create_group(*globalComm, *stageManagerGroup, groupTag, pipelineComm);
                    break; 
                }
            }

            // now check which job the process was assigned to (e.g., stage 1, stage 2, etc.)
            for(int stageID = 1; stageID <= pipeInfo.GetNumStages(); stageID++) {
                // check if process belongs to group
                int localRank;
                MPI_Group * stageProcessGroup = pipeInfo.GetStageProcessGroup(stageID);
                MPI_Group_rank(*stageProcessGroup, &localRank);

                // if the process belongs to the group, create the communicator
                if(localRank != MPI_UNDEFINED) { 
                    // processes will be assigned to their respective stage jobs
                    procInfo.SetProcJob(pipeInfo.GetNumStages() + 1 + stageID - 1);
                    procInfo.SetStageID(stageID);
                    
                    int groupTag = 2000 + pipeInfo.GetNumStages() + 1 + stageID; 

                    // create the group communicator
                    MPI_Comm * pipelineComm = pipeInfo.GetPipelineComm();
                    MPI_Comm_create_group(*globalComm, *stageProcessGroup, groupTag, pipelineComm);
                    break; 
                }
            }
        } 
        // here it means that processes were spawned during application execution
        // they have to learn about the pipeline configuration and indentify which job to execute
        else if(bufConfig[0] == 5){
            procInfo.SetIsLateSpawnProcess(true);

            int myStageID = 0;
            int isMsgReady;
            MPI_Request requestReconfig;
            MPI_Message probeMsg;
            MPI_Status status;
            int numRanks;

            for(int stageID = 1; stageID <= pipeInfo.GetNumStages(); stageID++) {
                // find which stage has missing processes so that we can take the job
                // the pipeline manager marked stages with missing processes with a 1
                if(bufConfig[stageID] == 1) {
                    myStageID = stageID;
                    procInfo.SetProcJob(pipeInfo.GetNumStages() + 1 + stageID - 1);
                    procInfo.SetStageID(myStageID);
                    break;
                }
            }

            // Recv and update the process stage group ranks with the new processes added in the stage
            for(int stageID = 1; stageID <= pipeInfo.GetNumStages(); stageID++) {
                do {
                    MPI_Improbe(0, RECONFIG_MSG, *globalComm, &isMsgReady, &probeMsg, &status);
                } while (!isMsgReady);
                MPI_Get_count(&status, MPI_INT, &numRanks);

                // The pipelineManager will send the new ranks of the processes added in the stage
                int newProcessStageRanks[numRanks];
                MPI_Imrecv(newProcessStageRanks, numRanks, MPI_INT, &probeMsg, &requestReconfig);
                MPI_Wait(&requestReconfig, MPI_STATUS_IGNORE);

                // Save the updated ranks in the corresponding stageID
                pipeInfo.SetProcessStageGroupRanks(stageID, newProcessStageRanks, numRanks);
                pipeInfo.UpdateStageSize();
            }        

            pipeInfo.UpdateStageRanksUsingGlobalComm(myStageID);

            // get the group of ProcessStageGroup we just set
            MPI_Group * stageProcessGroup = pipeInfo.GetStageProcessGroup(myStageID);

            int groupTag = 2000 + pipeInfo.GetNumStages() + 1 + myStageID; 

            // create the group communicator
            MPI_Comm * pipelineComm = pipeInfo.GetPipelineComm();
            MPI_Comm_create_group(*globalComm, *stageProcessGroup, groupTag, pipelineComm);
        }
    }

    void PipelineGraph::AddPipelineStage(Operator * stageProcess){
        stageList.push_back(stageProcess);
    }
}