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

class ProcessStatus {

public:
    bool alive;
    bool eos;
    bool special_eos;

    ProcessStatus() {
        alive = false;
        eos = false;
        special_eos = false;
    }

    void reset(){
        alive = false;
    }

    void setAlive(){
        alive = true;
    }

    bool getAlive(){
        return alive;
    }

    void setEOS(){
        eos = true;
    }

    bool getEOS(){
        return eos;
    }
    void setSpecialEOS(){
        special_eos = true;
    }
    bool getSpecialEOS(){
        return special_eos;
    }
};

struct StageStatus {
    int code;
    int myRank;
    int itemsConsumed;
    int itemsProduced;
};

struct Header{
    long msgID;
    int incomingMsgs;
};

template <typename TData>
struct StreamItem{
    Header header;
    TData payload;
};

template <typename TData>
struct StreamItemComparison{
	bool operator()(const StreamItem<TData> & t1, const StreamItem<TData> & t2){
		return (t1.header.msgID > t2.header.msgID);
	}
};

namespace mpr{
    struct None {};
}