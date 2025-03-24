# Dask workflow for denoising large images
A brief run-down of the Dask-based workflow for denoising large images.

## Setup
In a Python environment install the following packages:
`ndsafir>=4.1.1 dask[distributed] pylibczirw zarr<3`. If
you want to run the Jupyter notebooks then also install
`jupyter bokeh>=3.1.0 graphviz`.

E.g.
```
pip install 'ndsafir>=4.1.1' 'dask[distributed]' 'pylibczirw' 'zarr<3'
pip install 'jupyter' 'bokeh>=3.1.0 graphviz'
```
