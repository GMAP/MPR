SHELL		= /bin/bash
MPI_CXX 	= mpic++
CXX_DEFS 	=  -O3 

all: 

# Prime Numbers
prime_mpr: prime_numbers/prime_mpr.cpp
	$(MPI_CXX) $(CXX_DEFS) $< -o $@

# Mandelbrot Set
mandel_mpr: mandelbrot_set/mandel_mpr.cpp
	$(MPI_CXX) $(CXX_DEFS) $< -o $@

clean:
	rm -rf prime_mpr mandel_mpr