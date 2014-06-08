#!/bin/bash
python -m cProfile -o benchmark.pyprof ./searchengine.py;
pyprof2calltree -i benchmark.pyprof -o benchmark.out
kcachegrind benchmark.out 1> /dev/null 2> /dev/null &

