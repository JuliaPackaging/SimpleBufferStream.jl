export BufferStream
import Base: read, unsafe_read, readbytes!, readavailable, unsafe_write, write, wait, close, eof, isopen

"""
    BufferStream

Simple buffered stream that appends data to an internal chunk list and allows for easy,
simultaneous reading of that data.  Intended for shuffling data between tasks.  Best
performance is achieved by using `readavailable()`, which avoids copies on reads.
"""
mutable struct BufferStream <: IO
    chunks::Vector{Vector{UInt8}}
    chunk_read_idx::Int
    read_cond::Threads.Condition
    write_cond::Threads.Condition
    is_open::Bool

    BufferStream() = new(Vector{UInt8}[], 1, Threads.Condition(), Threads.Condition(), true)
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

function write(bs::BufferStream, data::Vector{UInt8})
    # Disallow writing if we're not open
    if !isopen(bs)
        throw(ArgumentError("Stream is closed"))
    end

    # Copy the data so that users can't clobber our internal list
    lock(bs.write_cond) do
        push!(bs.chunks, copy(data))
    end

    # Notify anyone who was waiting for some data
    lock(bs.read_cond) do
        notify(bs.read_cond; all=false)
    end
    return length(data)
end

# Helper methods
write(bs::BufferStream, x::UInt8) = write(bs, [x])
unsafe_write(bs::BufferStream, p::Ptr{UInt8}, len::UInt64) = write(bs, unsafe_wrap(Array, p, (len,)))

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
        chunk = lock(bs.write_cond) do
            popfirst!(bs.chunks)
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
        chunk = lock(bs.write_cond) do
            firstchunk = first(bs.chunks)

            # take view of only what we will read
            chunk = view(firstchunk, bs.chunk_read_idx:min(bs.chunk_read_idx + maxlen - 1,length(firstchunk)))

            # Update datastructure to consume the view we just took
            if maxlen > length(firstchunk) - bs.chunk_read_idx
                popfirst!(bs.chunks)
                bs.chunk_read_idx = 1
            else
                bs.chunk_read_idx += maxlen
            end
            return chunk
        end

        for idx in 1:length(chunk)
            data[idx] = chunk[idx]
        end
        return length(chunk)
    end
end