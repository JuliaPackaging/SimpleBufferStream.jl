export BufferStream, mem_usage, append_chunk
import Base: write, read, readbytes!, readavailable, unsafe_write, wait, close, eof, isopen, skip, length

"""
    BufferStream

Simple buffered stream that appends data to an internal chunk list and allows for easy,
simultaneous reading of that data.  Intended for shuffling data between tasks.  Best
performance is achieved by using `readavailable()`, which avoids copies on reads.
"""
mutable struct BufferStream <: IO
    chunks::Vector{AbstractVector{UInt8}}
    chunk_read_idx::Int
    read_cond::Threads.Condition
    write_cond::Threads.Condition
    is_open::Bool
    max_len::Int

    BufferStream(max_len::Int = 0) = new(AbstractVector{UInt8}[], 1, Threads.Condition(), Threads.Condition(), true, max_len)
end

# Close the stream, writing not allowed (but can still read until `eof()`)
function close(bs::BufferStream)
    bs.is_open = false
    lock(bs.read_cond) do
        notify(bs.read_cond)
    end
end

isopen(bs::BufferStream) = bs.is_open
eof(bs::BufferStream) = !isopen(bs) && isempty(bs.chunks)
function length(bs::BufferStream)
    lock(bs.write_cond) do
        if isempty(bs.chunks)
            return 0
        end
        len = length(bs.chunks[1]) - bs.chunk_read_idx + 1
        for idx in 2:length(bs.chunks)
            len += length(bs.chunks[idx])
        end
        return len
    end
end
function mem_usage(bs::BufferStream)
    lock(bs.write_cond) do
        return sum(Int[length(chunk) for chunk in bs.chunks])
    end
end

function append_chunk(bs::BufferStream, data::AbstractVector{UInt8})
    # Disallow writing if we're not open
    if !isopen(bs)
        throw(ArgumentError("Stream is closed"))
    end

    if bs.max_len != 0 && length(data) > bs.max_len
        throw(ArgumentError("Chunk is too large to fit into this BufferStream!"))
    end

    # Copy the data so that users can't clobber our internal list
    lock(bs.write_cond) do
        # If we would exceed our maximum length, then we must wait until someone reads from us.
        while bs.max_len != 0 && length(bs) + length(data) > bs.max_len
            wait(bs.write_cond)
        end
        push!(bs.chunks, data)
    end

    # Notify someone who was waiting for some data
    lock(bs.read_cond) do
        notify(bs.read_cond; all=false)
    end
    return length(data)
end

# Helper methods for read/write
write(bs::BufferStream, x::UInt8) = write(bs, [x])
function unsafe_write(bs::BufferStream, ref::Ptr{UInt8}, nbytes::UInt)
    return append_chunk(bs, copy(unsafe_wrap(Array, ref, (nbytes,))))
end

# Read a single byte.
function read(bs::BufferStream, ::Type{UInt8})
    data = UInt8[0]
    readbytes!(bs, data, 1)
    return data[1]
end

# Read everything
function read(bs::BufferStream)
    ret = UInt8[]
    lock(bs.read_cond) do
        while !eof(bs)
            append!(ret, readavailable(bs))
        end
    end
    return ret
end

# Completely consume the first chunk.
function readavailable(bs::BufferStream)
    lock(bs.read_cond) do
        if isempty(bs.chunks)
            if !isopen(bs)
                return UInt8[]
            end
            wait(bs.read_cond)
            
            # Handle cancellation explicitly
            if isempty(bs.chunks)
                return UInt8[]
            end
        end

        # We're gonna consume the rest of this chunk
        local chunk
        lock(bs.write_cond) do
            chunk = popfirst!(bs.chunks)
            notify(bs.write_cond; all=false)
        end
        chunkview = view(chunk, bs.chunk_read_idx:length(chunk))
        bs.chunk_read_idx = 1
        return chunkview
    end
end

# Read up to `maxlen` bytes, potentially partially consuming the first chunk.
function readbytes!(bs::BufferStream, data::Vector{UInt8}, maxlen::Int)
    lock(bs.read_cond) do
        if isempty(bs.chunks)
            if !isopen(bs)
                return 0
            end
            wait(bs.read_cond)

            # Handle cancellation explicitly
            if isempty(bs.chunks)
                return 0
            end
        end

        # Grab the first chunk that is available
        local chunk
        lock(bs.write_cond) do
            firstchunk = first(bs.chunks)

            # take view of only what we will read
            chunk = view(firstchunk, bs.chunk_read_idx:min(bs.chunk_read_idx + maxlen - 1,length(firstchunk)))

            # Update datastructure to consume the view we just took
            if maxlen > length(firstchunk) - bs.chunk_read_idx
                popfirst!(bs.chunks)
                bs.chunk_read_idx = 1
                notify(bs.write_cond; all=false)
            else
                bs.chunk_read_idx += maxlen
            end
        end

        for idx in 1:length(chunk)
            data[idx] = chunk[idx]
        end
        return length(chunk)
    end
end

function skip(bs::BufferStream, n::Int)
    data = Array{UInt8}(undef, min(2*1024*1024, n))
    while n > 0
        n -= readbytes!(bs, data, min(length(data), n))
        if eof(bs)
            break
        end
    end
    return n
end
