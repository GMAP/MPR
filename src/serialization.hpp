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
    class Serialization
    {
    public:
        MPI_Datatype MPI_PROCESS_STATUS;
        MPI_Datatype MPI_STAGE_STATUS;

        Serialization();
        ~Serialization();
    };
    
    Serialization::Serialization() {
        int count = 3;
        int array_of_blocklengths[] = { 1, 1, 1 };
        MPI_Aint array_of_displacements[] = { offsetof( ProcessStatus, alive ),
                                            offsetof( ProcessStatus, eos ), 
                                            offsetof( ProcessStatus, special_eos )
                                            };
        MPI_Datatype array_of_types[] = { MPI_C_BOOL, MPI_C_BOOL, MPI_C_BOOL };
        MPI_Type_create_struct( count, array_of_blocklengths, array_of_displacements, array_of_types, &MPI_PROCESS_STATUS );
        MPI_Type_commit( &MPI_PROCESS_STATUS );

        
        int count2 = 4;
        int array_of_blocklengths2[] = { 1, 1, 1, 1 };
        MPI_Aint array_of_displacements2[] = { offsetof( StageStatus, code ),
                                            offsetof( StageStatus, myRank ),
                                            offsetof( StageStatus, itemsConsumed ),
                                            offsetof( StageStatus, itemsProduced )
                                            };
        MPI_Datatype array_of_types2[] = { MPI_INT, MPI_INT, MPI_INT, MPI_INT };
        MPI_Type_create_struct( count2, array_of_blocklengths2, array_of_displacements2, array_of_types2, &MPI_STAGE_STATUS );
        MPI_Type_commit( &MPI_STAGE_STATUS );
    }
    
    Serialization::~Serialization() {

    }
}