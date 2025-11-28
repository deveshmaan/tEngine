import json
import sys
from pathlib import Path

if len(sys.argv) != 3:
    raise SystemExit("usage: py_to_ipynb.py <src.py> <out.ipynb>")

src = Path(sys.argv[1])
out = Path(sys.argv[2])

cells = []
cell_lines = []
cell_type = None

def flush():
    if not cell_lines:
        return
    text = ''.join(cell_lines)
    source = text.splitlines(keepends=True)
    cells.append({
        "cell_type": cell_type or "code",
        "metadata": {},
        "source": source,
        "outputs": [] if (cell_type or "code") == "code" else None,
        "execution_count": None if (cell_type or "code") == "code" else None,
    })

with src.open() as fh:
    for line in fh:
        if line.startswith('# %%'):
            flush()
            cell_lines = []
            cell_type = 'markdown' if 'markdown' in line else 'code'
            continue
        if cell_type == 'markdown':
            if line.startswith('# '):
                cell_lines.append(line[2:])
            elif line.startswith('#'):
                cell_lines.append(line[1:])
            else:
                cell_lines.append(line)
        else:
            cell_lines.append(line)
flush()

# Clean outputs for markdown cells
for cell in cells:
    if cell['cell_type'] == 'markdown':
        cell.pop('outputs', None)
        cell.pop('execution_count', None)

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python", "pygments_lexer": "ipython3", "codemirror_mode": {"name": "ipython", "version": 3}}
    },
    "nbformat": 4,
    "nbformat_minor": 5
}

with out.open('w') as fh:
    json.dump(nb, fh, indent=1)
