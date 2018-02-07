#!/bin/bash
strace -fvp `pgrep -f "Layer"` -c
