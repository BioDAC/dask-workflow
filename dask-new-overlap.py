#!/usr/bin/env python
import collections
import dask
from dask.distributed import Client
import dask.distributed
from dask_jobqueue.slurm import SLURMRunner
import itertools
import math
import ndsafir
import numpy as np
from pylibCZIrw import czi
import zarr


# Class to hold values corresponding to T, Z, Y, X axes.
TZYX = collections.namedtuple("TZYX", ["T", "Z", "Y", "X"])

class TCZYX(collections.namedtuple("TCZYX", ["T", "C", "Z", "Y", "X"])):
    """
    Class to hold values corresponding to T, C, Z, Y, X axes,
    as might be in an image.
    """
    @property
    def TZYX(self):
        return TZYX(self.T, self.Z, self.Y, self.X)


def range_contains(r_inner: range, r_outer: range):
    """
    Determines whether r_inner is entirely present in r_outer.
    Both parameters must be ranges.
    """

    # Empty set is always contained.
    if len(r_inner) == 0:
        return True

    # Inner start must be there.
    if r_inner.start not in r_outer:
        return False
    if len(r_inner) == 1:
        return True

    # Actual last element of r_inner, must be there.
    if r_inner[-1] not in r_outer:
        return False
    if len(r_inner) == 2:
        return True

    return (r_inner.step % r_outer.step) == 0


@dask.delayed
def load_chunk(input_filepath, t_range, z_range, y_range, x_range, scene):
    """
    Load slices of the input file, as given by the ranges
    from scene `scene`.

    Returns a ndarray copy of the slice, in TCZYX order.
    """
    with czi.open_czi(input_filepath) as input_file:
        bbox = input_file.total_bounding_box
        C = bbox["C"][1] - bbox["C"][0]
        # The ROI can go beyond our scene without error.
        # We will check here and raise an error if we do so.
        bound_rect = input_file.scenes_bounding_rectangle.get(scene, None)
        if not bound_rect:
            bound_rect = input_file.total_bounding_rectangle
        X = range(bound_rect[0], bound_rect[0] + bound_rect[2])
        Y = range(bound_rect[1], bound_rect[1] + bound_rect[3])
        if not range_contains(y_range, Y) or not range_contains(x_range, X):
            raise ValueError("input range lies outside image")

        # Can query input_file.pixel_types but assume uint16
        data = np.empty(shape=(len(t_range), C, len(z_range), len(y_range), len(x_range)), dtype=np.uint16)
        roi = (x_range.start, y_range.start, len(x_range), len(y_range))
        ndarray_t = 0
        for t in t_range:
            ndarray_z = 0
            for z in z_range:
                # NB input_file zero indexed
                data[ndarray_t, :, ndarray_z, :, :] = np.moveaxis(
                    input_file.read(
                        plane={"T": t, "Z": z},
                        scene=scene,
                        roi=roi,
                    ),
                    2,
                    0
                )
                ndarray_z += 1
            ndarray_t += 1
    return data


@dask.delayed
def process_block(block):
    """
    Denoise a block, then return it
    """
    denoised = ndsafir.denoise(
        block,
        mode="poisson-gaussian",
        gains=[3.92],
        offsets=[-388],
        patch=[0, 0, 1, 3, 3],
        max_iter=4,
        pvalue=0.1,
        nthreads=74,
        axes="TCZYX",
    )
    return denoised


def get_czi_scene_shape(filename, scene=0):
    """
    Return a TCZYX containing the sizes of the CZI file in
    those axes.
    """
    with czi.open_czi(filename) as input_file:
        bbox = input_file.total_bounding_box
        C = bbox["C"][1] - bbox["C"][0]
        T = bbox["T"][1] - bbox["T"][0]
        Z = bbox["Z"][1] - bbox["Z"][0]

        bound_rect = input_file.scenes_bounding_rectangle.get(scene, None)
        if not bound_rect:
            bound_rect = input_file.total_bounding_rectangle
        width = bound_rect.w
        height = bound_rect.h
    return TCZYX(T, C, Z, height, width)


def load_chunks_from_czi(filename, chunk_sizes: TZYX, overlap: TZYX,
                         existing_chunks, chunk_ratios, scene=0):
    """
    A function that takes a filename and a chunking regimen and returns dict
    made up of chunks of the appropriate size.  The chunks will include
    overlaps as per `overlap`.

    Checks to see if each chunk is wholly present in the existing chunks.

    Params:
    filename: CZI file
    chunk_sizes: TZYX Contains chunk dimensions in those axes.
    overlap: TZYX Contains overlap dimensions.
    scene: CZI scene to load.
    existing_chunks: set(TZYX) Chunks present in zarr file.
    chunk_ratios: TZYX Ratio of dask chunk / zarr chunk sizes per axis.
                       Must be integral.

    Returns:
    dict[(T_c, Z_c, Y_c, X_c)] = block, where T_c, etc, are indices in the
        overall array in chunk units.
    """
    with czi.open_czi(filename) as input_file:
        bbox = input_file.total_bounding_box
        C = bbox["C"][1] - bbox["C"][0]
        T = bbox["T"][1] - bbox["T"][0]
        Z = bbox["Z"][1] - bbox["Z"][0]

        bound_rect = input_file.scenes_bounding_rectangle.get(scene, None)
        if not bound_rect:
            bound_rect = input_file.total_bounding_rectangle
        X_min = bound_rect.x
        width = bound_rect.w
        Y_min = bound_rect.y
        height = bound_rect.h

        chunk_dict = {}
        t_ind = 0
        for t in range(0, T, chunk_sizes.T):
            z_ind = 0
            for z in range(0, Z, chunk_sizes.Z):
                y_ind = 0
                for y in range(Y_min, Y_min + height, chunk_sizes.Y):
                    x_ind = 0
                    for x in range(X_min, X_min + width, chunk_sizes.X):
                        t_range = range(max(t - overlap.T, 0), min(T, t + chunk_sizes.T + overlap.T))
                        z_range = range(max(z - overlap.Z, 0), min(Z, z + chunk_sizes.Z + overlap.Z))
                        y_range = range(max(y - overlap.Y, Y_min), min(Y_min + height, y + chunk_sizes.Y + overlap.Y))
                        x_range = range(max(x - overlap.X, X_min), min(X_min + width, x + chunk_sizes.X + overlap.X))
                        #print("ranges: ",
                        #    [f"{r}({len(r)})" for r in (t_range, z_range, y_range, x_range)]
                        #)
#                        chunk_dict[t_idx, 0, z_idx, y_idx, x_idx] = load_chunk(

                        # Check output array here
                        if not check_existing_chunks((t_ind, z_ind, y_ind, x_ind), chunk_ratios, existing_chunks):
                            chunk_dict[(t_ind, z_ind, y_ind, x_ind)] = \
                                load_chunk(filename, t_range, z_range, y_range, x_range, scene=scene)
                            dask.distributed.print(f"Added {(t_ind, z_ind, y_ind, x_ind)}")
                        x_ind += 1
                    y_ind += 1
                z_ind += 1
            t_ind += 1
    return chunk_dict


def get_chunk_dims(file_values, chunk_sizes):
    """
    Work out how many chunks in each dimension, return
    a TZYX with these values.

    Param:
    file_values: TCZYX The dimensions of the file.
    chunk_sizes: TZYX Chunk sizes.

    Returns:
    TZYX: How many chunks in each dimension.
    """
    ch_shape = [math.ceil(f/c) for (f, c) in zip(file_values.TZYX, chunk_sizes)]
    return TZYX(*ch_shape)


@dask.delayed
def trim(block, ch_index, chunk_array, overlaps):
    """
    Trim off the overlap data from the block.

    Params:
    block: np.ndarray The data to trim.
    ch_index: TZYX The position in the overall array, in chunk units.
    chunk_array: TZYX The size of the overall array, in chunk units.
    overlap: TZYX The size of the overlaps in each axis.

    Returns:
    np.ndarray: Trimmed data.
    """
    axes = []
    for i, c_i in enumerate(ch_index):
        lo = overlaps[i] if c_i > 0 else 0
        hi = -overlaps[i] if overlaps[i] > 0 and c_i < chunk_array[i]-1 else None
        axes.append(slice(lo, hi))
    print(axes)
    axes.insert(1, slice(0, None)) # C
    return block[*axes]


@dask.delayed
def save_block_to_zarr(block, zarr_filename, chunk_index, chunk_sizes):
    """
    Save the block to output zarr array.

    Params:
    block: np.ndarray Block to save.
    zarr_filename: str Filename of zarr array.
    chunk_index: TZYX The position in the overall array, in chunk units.
    chunk_size: TZYX Chunk size.

    Returns:
    nothing.
    """

    # for now assume zarr_filename exists and is a suitable zarr array
    output_zarr = zarr.convenience.open(zarr_filename, mode="r+")
    section = list(map(lambda t: slice(t[0] * t[1], t[0] * t[1] + t[2]), zip(chunk_index, chunk_sizes, TCZYX(*block.shape).TZYX)))
    section.insert(1, slice(0, None))
    dask.distributed.print(f"Saving section {section}")
    output_zarr[*section] = block


def check_existing_chunks(da_chunk: TZYX, chunk_ratios, existing_chunks):
    """
    Check to see if all necessary chunks are present in existing_chunks
    (a set of tuples of TZYX).  It checks all zarr chunks that contribute
    to a dask chunk.  It stops early and uses a set for checking.

    The chunk ratios must be integral.

    da_chunk: TZYX is the dask chunk
    chunk_ratios: TZYX dask_chunk_size / zarr_chunk size for each axis
    existing_chunks: set(TZYX) set of existing chunks
    """
    ranges = []
    for da_ch, ch_r in zip(da_chunk, chunk_ratios):
        ranges.append(range(da_ch * ch_r, (da_ch+1) * ch_r))
    return all(
        map(
            lambda t: t in existing_chunks,
            itertools.product(*ranges)
        )
    )

# OK, this appears to be the best way to do this?
dask.config.set({"distributed.scheduler.worker-saturation": 1.0})

with SLURMRunner(
    scheduler_file="/rds/user/sjc306/hpc-work/scheduler-{job_id}.json",
    worker_options={
        "interface": "ib0",
        "local_directory": "/local/sjc306",
        "memory_limit": "240GiB",
        "nthreads": 1,
    },
    scheduler_options={
        "dashboard": True,
    },
) as runner:

    with Client(runner) as client:
        """
        From here on the code only runs on the dask Client.
        """

        # PARAMETERS
        # Input file
        # filename = "/rds/project/rds-1FbiQayZlSY/data/Millie/Timepoint3-02-Embryo3-Lattice Lightsheet.czi"
        filename = "/rds/project/rds-1FbiQayZlSY/data/Millie/Millie 24Aug23 raw light sheet data/Timepoint3-02.czi"
        scene = 0
        # Dask chunks
        t_chunk_size = 17
        z_chunk_size = 1501
        y_chunk_size = 512
        x_chunk_size = 484
        # Overlaps
        overlaps = TZYX(2, 4, 16, 16)
        # Output file
        output_filename = "/rds/project/rds-1FbiQayZlSY/data/Millie/Timepoint3-denoised-v2"
        output_chunks=TCZYX(1, 1, 1501, 512, 484)

        chunk_sizes = TZYX(t_chunk_size, z_chunk_size, y_chunk_size, x_chunk_size)
        czi_file = czi.CziReader(filename)
        file_array = get_czi_scene_shape(filename, scene)
        print(f"scenes_bounding_rectangle: {czi_file.scenes_bounding_rectangle}")
        print(f"\ntotal_bounding_box: {czi_file.total_bounding_box}")

        chunk_array = get_chunk_dims(file_array, chunk_sizes)

        # Check for existence of output array.  Open it if it exists to get
        # list of existing blocks.
        try:
            output_zarr = zarr.open_array(
                output_filename,
                mode="r",
            )
        except zarr.errors.ArrayNotFoundError:
            output_zarr = zarr.create(
                store=output_filename,
                shape=file_array,
                dtype=np.uint16,
                chunks=output_chunks,
            )

        # Check sizes and that zarr chunks are integral multiples of dask chunks.
        # Might also be worth checking that the dask array will fit in the zarr array!
        output_zarr_chunks = TCZYX(*output_zarr.chunks)
        for f_ch, d_ch in zip(output_zarr_chunks.TZYX, chunk_sizes):
            if f_ch > d_ch:
                print(chunk_sizes)
                print(output_zarr.chunks)
                raise ValueError("Dask chunks smaller than output chunks")
            if d_ch % f_ch != 0:
                print(chunk_sizes)
                print(output_zarr.chunks)
                raise ValueError("Dask chunks not multiples of output chunks")

        # Make set of existing chunks
        existing_chunks = {
            TCZYX(*map(int, k.split('.'))).TZYX for k in output_zarr.store.keys() if k != ".zarray"
        }
        # Get ratios of dask to file chunks.  Must be integral.
        chunk_ratios = TZYX(*[ d_c // f_c for (d_c, f_c) in zip(chunk_sizes, output_chunks.TZYX) ])
        
        # Load the chunks.
        data_array = load_chunks_from_czi(filename, chunk_sizes, overlaps,
                                          existing_chunks, chunk_ratios)

        # Denoise each chunk.
        blocked_array = {
                k: process_block(v) for (k, v) in data_array.items()
        }

        # Trim the overlap from the chunks.
        trimmed_array = {
            b_ind: trim(block, b_ind, chunk_array, overlaps)
                for (b_ind, block) in blocked_array.items()
        }

        # Save the data to the output zarr.
        final_result = [
            save_block_to_zarr(block, output_filename, b_ind, chunk_sizes)
                for (b_ind, block) in trimmed_array.items()
        ]


        # Start the actual calculation!  Final result will be an array
        # of `None`, one for each chunk.
        dask.compute(*final_result)
