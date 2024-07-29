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

#include <string>
#include <iostream>
#include <sstream>
#include <functional>
#include <map>
#include <sys/time.h>
#include <math.h>
#include <chrono>

#include "../../src/mpr.hpp"

#define DIM 1024

int write_bmp(const char *filename, int width, int height, char ** image);

struct LineToRender
{
	int line;
	char *M;
};

class Source : public mpr::StageProcess<void*, int> {
private:
	int numberOfLines;
public:
	Source(int _numberOfLines) {
		numberOfLines = _numberOfLines;
	};

    void OnInit(void * ctx) {}
    void OnEnd(void * ctx) {}
    void OnInput(void * ctx, void * data) {}

	void OnProduce(void * ctx) {
		for (int i = 0; i < numberOfLines; i++)
		{
			Produce(ctx, (void*)&i, sizeof(int));
		}
	};
};


class Compute : public mpr::StageProcess<int, LineToRender> {
private:
	int dimensions;
	double init_a;
	double init_b;
	double step;
	int niter;
public:
	Compute(int _dimensions, double _init_a, double _init_b, double _step, int _niter) {
		dimensions = _dimensions;
		init_b = _init_b;
		init_a = _init_a;
		step = _step;
		niter = _niter;
	};

    void OnInit(void * ctx) {}
    void OnEnd(void * ctx) {}
    void OnProduce(void * ctx) {}
	
	void OnInput(void * ctx, void * data) {
        int * line = static_cast<int*>(data);
		int dim = dimensions;
		char *M = new char[dim*3];
		int i = *line;
		double im;
		im = init_b + (step * i);
		for (int j = 0; j < dim; j++)
		{
			double cr;
			double a = cr = init_a + step * j;
			double b = im;
			int k = 0;
			for (; k < niter; k++)
			{
				double a2 = a * a;
				double b2 = b * b;
				if ((a2 + b2) > 4.0)
					break;
				b = 2 * a * b + im;
				a = a2 - b2 + cr;
			}
			if(k == niter) {
    			M[j*3] = 0;
    			M[j*3 + 1] = 0;
    			M[j*3 + 2] = 0;
			} else {
				M[j*3] = k;
				M[j*3 + 1] = k;
				M[j*3 + 2] = k;
			}
		}

        std::vector<void *> dataOutList;
        std::vector<int> sizeList;
        
		LineToRender lineToRender;
		lineToRender.line = i;

        dataOutList.push_back((void*)&lineToRender);
        sizeList.push_back(sizeof(LineToRender));

        dataOutList.push_back((void*)M);
        sizeList.push_back(dimensions*3);

        PublishMulti(ctx, dataOutList, sizeList);

        delete[] M;
	};
};


class Sink : public mpr::StageProcess<LineToRender, void*> {
	char **image;
public:

    void OnProduce(void * ctx) {}

    void OnInit(void * ctx) {
		image = new char*[DIM];
        for (int i = 0; i < DIM; i++)
            image[i] = new char[DIM * 3];
	}

	void OnInput(void * ctx, void * data) {
        LineToRender * dataIn = static_cast<LineToRender*>(data);
        memcpy(image[dataIn->line], dataIn->M, DIM * 3);
	};

    void OnEnd(void * ctx) { 
		write_bmp("output.bmp", DIM, DIM, image); 
        cout << "Image generated successfully! \nSaved in root dir as output.bmp" << endl;
        delete[] image;
	}
};

int main(int argc, char *argv[]) {
	if (argc < 2) {
		printf("./executable numIterations\n");
		return -1;
	}

	int niter = atoi(argv[1]);

	double init_a = -2.125, init_b = -1.5, range = 3.0;
	int dim = DIM;
	double step = range / (double)dim;
        
    mpr::PipelineGraph pipe(&argc, &argv, 3);

	Source source(dim);
	Compute compute(dim, init_a, init_b, step, niter);
	Sink sink;

    pipe.AddPipelineStage(&source);
    pipe.AddPipelineStage(&compute);
    pipe.AddPipelineStage(&sink);

    pipe.Run();

	return 0;
}


// --- Auxiliary functions to write the image

struct BMPHeader
{
    char bfType[2]; /* "BM" */
    int bfSize; /* Size of file in bytes */
    int bfReserved; /* set to 0 */
    int bfOffBits; /* Byte offset to actual bitmap data (= 54) */
    int biSize; /* Size of BITMAPINFOHEADER, in bytes (= 40) */
    int biWidth; /* Width of image, in pixels */
    int biHeight; /* Height of images, in pixels */
    short biPlanes; /* Number of planes in target device (set to 1) */
    short biBitCount; /* Bits per pixel (24 in this case) */
    int biCompression; /* Type of compression (0 if no compression) */
    int biSizeImage; /* Image size, in bytes (0 if no compression) */
    int biXPelsPerMeter; /* Resolution in pixels/meter of display device */
    int biYPelsPerMeter; /* Resolution in pixels/meter of display device */
    int biClrUsed; /* Number of colors in the color table (if 0, use
maximum allowed by biBitCount) */
    int biClrImportant; /* Number of important colors. If 0, all colors
are important */
};
int write_bmp(const char *filename, int width, int height, char ** image)
{
    int i, j, ipos;
    int bytesPerLine;
    unsigned char *line;

    FILE *file;
    struct BMPHeader bmph;

    /* The length of each line must be a multiple of 4 bytes */
    bytesPerLine = (3 * (width + 1) / 4) * 4;


    strncpy(bmph.bfType, "BM", 2);
    bmph.bfOffBits = 54;
    bmph.bfSize = bmph.bfOffBits + bytesPerLine * height;
    bmph.bfReserved = 0;
    bmph.biSize = 40;
    bmph.biWidth = width;
    bmph.biHeight = height;
    bmph.biPlanes = 1;
    bmph.biBitCount = 24;
    bmph.biCompression = 0;
    bmph.biSizeImage = bytesPerLine * height;
    bmph.biXPelsPerMeter = 0;
    bmph.biYPelsPerMeter = 0;
    bmph.biClrUsed = 0;
    bmph.biClrImportant = 0;

    file = fopen (filename, "wb");
    if (file == NULL) return(0);

    fwrite(&bmph.bfType, 2, 1, file);
    fwrite(&bmph.bfSize, 4, 1, file);
    fwrite(&bmph.bfReserved, 4, 1, file);
    fwrite(&bmph.bfOffBits, 4, 1, file);
    fwrite(&bmph.biSize, 4, 1, file);
    fwrite(&bmph.biWidth, 4, 1, file);
    fwrite(&bmph.biHeight, 4, 1, file);
    fwrite(&bmph.biPlanes, 2, 1, file);
    fwrite(&bmph.biBitCount, 2, 1, file);
    fwrite(&bmph.biCompression, 4, 1, file);
    fwrite(&bmph.biSizeImage, 4, 1, file);
    fwrite(&bmph.biXPelsPerMeter, 4, 1, file);
    fwrite(&bmph.biYPelsPerMeter, 4, 1, file);
    fwrite(&bmph.biClrUsed, 4, 1, file);
    fwrite(&bmph.biClrImportant, 4, 1, file);

    for (i = height - 1; i >= 0; i--)
    {
        fwrite(image[i], bytesPerLine, 1, file);
    }

    fclose(file);

    return(1);
}