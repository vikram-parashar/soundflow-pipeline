#!/bin/bash

cd etl/bronze
uv run extract.py
cd ../silver
uv run transform.py
cd ../gold
uv run transform.py
